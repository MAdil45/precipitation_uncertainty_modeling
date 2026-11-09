

"""
Download selected HRRR fields with HTTP byte-range requests.

We will read the remote HRRR index, calculate each GRIB message's byte range, select exactly one APCP,
2-m temperature, and 2-m relative-humidity record, and concatenate those three
unchanged GRIB messages into a smaller GRIB2 file.

First edit ``config.py`` and then run:
    python download_hrrr.py
"""

import csv
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from eccodes import codes_get, codes_grib_new_from_file, codes_release
from tqdm import tqdm

import config


logger = logging.getLogger("hrrr_downloader")
manifest_fields = [
    "valid_time_utc", "initialization_time_utc", "source_date", "cycle_hour_utc",
    "forecast_hour", "source_url", "local_path", "selectors_json", "ranges_json",
    "remote_file_bytes", "local_file_bytes", "status", "attempts", "error",
    "starts_with_grib", "ends_with_7777", "message_count", "metadata_checks_passed",
    "completed_utc",
    ]


def setup_logging():
    """
    Log progress both to the terminal and the raw HRRR directory.

    """

    # ensure the directories exist  
    config.output_root.mkdir(parents=True, exist_ok=True)
    config.log_path.parent.mkdir(parents=True, exist_ok=True)

    # configure logging to both file and terminal
    logging.basicConfig(level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(config.log_path), logging.StreamHandler()])


def parse_utc(value):
    """
    Parse one ISO timestamp and require an exact UTC clock hour.

    """
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"Timestamp must include UTC: {value}")
    
    parsed = parsed.astimezone(timezone.utc)
    if parsed.minute or parsed.second or parsed.microsecond:
        raise ValueError(f"Timestamp must be an exact clock hour: {value}")
    
    return parsed


def valid_hours(start, end):
    """
    Yield every UTC valid time in the inclusive configured range.

    """
    if end < start:
        raise ValueError("end_valid_time is before start_valid_time")
    
    current = start

    while current <= end:
        yield current
        current += timedelta(hours=1)


def source_details(valid_time):
    """
    Map a valid hour to its preceding HRRR cycle and f01 source URL.

    """
    initialization = valid_time - timedelta(hours=config.forecast_hour)
    
    source_date = initialization.strftime("%Y%m%d")
    cycle_hour = initialization.hour
    
    filename = f"hrrr.t{cycle_hour:02d}z.{config.product}f{config.forecast_hour:02d}.grib2"
    url = f"{config.aws_base_url}/hrrr.{source_date}/{config.region}/{filename}"
    
    directory = config.output_root / initialization.strftime("%Y/%m/%d")
    output_name = f"hrrr.t{cycle_hour:02d}z.{config.product}f{config.forecast_hour:02d}.selected.grib2"
    
    return initialization, source_date, cycle_hour, url, directory / output_name


def request_with_retries(session, method, url, **kwargs):
    """
    Send one HTTP request with bounded retries and increasing backoff.

    """
    last_error = None

    for attempt in range(1, config.max_retries + 1):
        try:
            response = session.request(
                method,
                url,
                timeout=(config.connect_timeout_seconds, config.read_timeout_seconds),
                **kwargs)
            
            response.raise_for_status()
            return response, attempt
        
        except requests.RequestException as error:
            last_error = error
            if attempt < config.max_retries:
                time.sleep(config.retry_backoff_seconds * attempt)

    raise RuntimeError(f"Request failed after {config.max_retries} attempts: {last_error}")


def read_remote_hrrr_index(session, grib_url):
    """
    Read the remote HRRR index and calculate inclusive message byte ranges.

    This is the same calculation used in the HRRR understanding notebook. The
    next message's start byte minus one is the current message's end byte.
    """

    index_response, index_attempts = request_with_retries(session, "GET", grib_url + ".idx")
    head_response, head_attempts = request_with_retries(session, "HEAD", grib_url)
    complete_file_size = int(head_response.headers["Content-Length"])
    
    records = []

    for line in index_response.text.splitlines():

        line = line.strip()
        if not line: continue

        parts = line.split(":")

        if len(parts) < 6:
            raise ValueError(f"Unexpected HRRR index line: {line}")
        
        records.append({
            "message": int(parts[0]),
            "start_byte": int(parts[1]),
            "reference_time": parts[2].removeprefix("d="),
            "variable": parts[3],
            "level": parts[4],
            "time_description": parts[5],
            "original_index_line": line,
            })

    if not records:
        raise ValueError(f"The remote index is empty: {grib_url}.idx")
        
    records.sort(key=lambda record: record["start_byte"])
    for index, record in enumerate(records):
        next_start = records[index + 1]["start_byte"] if index + 1 < len(records) else complete_file_size
        record["end_byte"] = next_start - 1
        
    return records, complete_file_size, index_attempts + head_attempts


def select_hrrr_records(index_records):
    """
    Select exactly one index record for each configured HRRR field.

    """

    selected = []

    for field_name, rule in config.selected_variables.items():
        matches = [
            record for record in index_records
            if record["variable"] == rule["variable"]
            and record["level"] == rule["level"]
            and record["time_description"] == rule["time_description"]
            ]
        
        if len(matches) != 1:
            raise ValueError(f"{field_name}: expected one index record, found {len(matches)}")
        
        selected_record = matches[0].copy()
        selected_record["field_name"] = field_name
        selected.append(selected_record)
    
    return sorted(selected, key=lambda record: record["start_byte"])


def download_grib_ranges(session, grib_url, records, destination):
    """
    Download selected GRIB messages and atomically publish the completed file.

    """

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_suffix(destination.suffix + ".part")
    attempts = 0

    with temporary_path.open("wb") as output_file:
        for record in records:
            
            byte_range = f"bytes={record['start_byte']}-{record['end_byte']}"
            response, request_attempts = request_with_retries(session, "GET", grib_url, headers={"Range": byte_range})
            
            attempts += request_attempts
            if response.status_code != 206:
                raise RuntimeError(f"Server did not return HTTP 206 for {record['field_name']}: {response.status_code}")
            
            expected_size = record["end_byte"] - record["start_byte"] + 1
            
            if len(response.content) != expected_size:
                raise IOError(
                    f"{record['field_name']}: expected {expected_size:,} bytes, "
                    f"received {len(response.content):,}"
                    )
            
            output_file.write(response.content)
    temporary_path.replace(destination)
    return attempts


def grib_datetime(date_value, time_value):
    """
    Convert ecCodes date and time values to a UTC datetime.

    """
    return datetime.strptime(f"{int(date_value)}{int(time_value):04d}", 
                             "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def inspect_selected_file(path, initialization, valid_time):
    """
    Verify GRIB framing, message identities, units, grid, and expected times.

    """
    starts_with_grib = False
    ends_with_7777 = False

    if path.stat().st_size >= 8:
        with path.open("rb") as stream:
            starts_with_grib = stream.read(4) == b"GRIB"
            stream.seek(-4, 2)
            ends_with_7777 = stream.read(4) == b"7777"

    expected = {
        "apcp": ("tp", "kg m**-2", "surface", 0, "accum", 0, 1),
        "temperature_2m": ("2t", "K", "heightAboveGround", 2, "instant", 1, 1),
        "relative_humidity_2m": ("2r", "%", "heightAboveGround", 2, "instant", 1, 1),
        }
    
    field_by_short_name = {details[0]: field_name for field_name, details in expected.items()}
    metadata_checks = []
    grids = []
    message_count = 0

    with path.open("rb") as stream:
        while True:
            message = codes_grib_new_from_file(stream)
            if message is None:
                break
            try:
                if message_count >= len(expected):
                    metadata_checks.append(False)
                    message_count += 1
                    continue

                # Byte-range messages are written in source-file order, which
                # is not necessarily the order used in SELECTED_VARIABLES.
                actual_short_name = codes_get(message, "shortName")
                field_name = field_by_short_name.get(actual_short_name)

                if field_name is None:
                    metadata_checks.append(False)
                    message_count += 1
                    continue

                short_name, units, level_type, level, step_type, start_step, end_step = expected[field_name]
                actual_initialization = grib_datetime(codes_get(message, "dataDate"), codes_get(message, "dataTime"))
                actual_valid = grib_datetime(codes_get(message, "validityDate"), codes_get(message, "validityTime"))
                
                metadata_checks.append(all([
                    actual_short_name == short_name,
                    codes_get(message, "units") == units,
                    codes_get(message, "typeOfLevel") == level_type,
                    codes_get(message, "level") == level,
                    codes_get(message, "stepType") == step_type,
                    codes_get(message, "startStep") == start_step,
                    codes_get(message, "endStep") == end_step,
                    actual_initialization == initialization,
                    actual_valid == valid_time,
                    ]))
                
                grids.append(tuple(codes_get(message, key) for key in ("gridType", "Nx", "Ny", "DxInMetres", "DyInMetres")))
                message_count += 1

            finally:
                codes_release(message)

    metadata_passed = (
        message_count == len(config.selected_variables)
        and len(metadata_checks) == len(config.selected_variables)
        and all(metadata_checks)
        and len(set(grids)) == 1
        )
    
    return {
        "starts_with_grib": starts_with_grib,
        "ends_with_7777": ends_with_7777,
        "message_count": message_count,
        "metadata_checks_passed": metadata_passed,
        "passed": starts_with_grib and ends_with_7777 and metadata_passed,
        }


def load_manifest(path):
    """
    Load prior manifest rows by valid time so interrupted runs can resume.

    """
    if not path.exists():
        return {}
    with path.open("r", newline="", encoding="utf-8") as stream:
        return {row["valid_time_utc"]: row for row in csv.DictReader(stream)}


def save_manifest(path, rows):
    """
    Atomically rewrite the manifest after each processed valid hour.

    """
    
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".part")
   
    with temporary_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=manifest_fields)
        writer.writeheader()
        writer.writerows(rows[key] for key in sorted(rows))
    temporary_path.replace(path)


def iso_utc(value):
    """
    Format a timezone-aware datetime with a compact UTC suffix.

    """
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def process_hour(session, valid_time, previous_row=None):
    """
    Download or validate one valid hour and return its complete manifest row.

    """
    initialization, source_date, cycle_hour, source_url, local_path = source_details(valid_time)
    
    row = {field: "" for field in manifest_fields}
    row.update({
        "valid_time_utc": iso_utc(valid_time),
        "initialization_time_utc": iso_utc(initialization),
        "source_date": source_date,
        "cycle_hour_utc": cycle_hour,
        "forecast_hour": config.forecast_hour,
        "source_url": source_url,
        "local_path": str(local_path),
        "selectors_json": json.dumps(config.selected_variables, sort_keys=True),
        "status": "in_progress",
        "attempts": 0,
        })

    try:
        # a completed existing file is decoded again before it is skipped. This
        # prevents a stale manifest entry from hiding a damaged local file.
        if previous_row and previous_row.get("status") == "complete" and local_path.is_file():
            integrity = inspect_selected_file(local_path, initialization, valid_time)
            
            if integrity["passed"]:
                row.update(previous_row)
                logger.info("Already complete: %s", row["valid_time_utc"])
                return row

        index_records, remote_size, index_attempts = read_remote_hrrr_index(session, source_url)
        selected_records = select_hrrr_records(index_records)
        range_attempts = download_grib_ranges(session, source_url, selected_records, local_path)
        
        integrity = inspect_selected_file(local_path, initialization, valid_time)
        if not integrity["passed"]:
            raise ValueError(f"Downloaded GRIB integrity checks failed: {integrity}")

        ranges = [
            {
            "field_name": record["field_name"],
            "message": record["message"],
            "start_byte": record["start_byte"],
            "end_byte": record["end_byte"],
            "original_index_line": record["original_index_line"],
            }
            for record in selected_records
            ]
        
        row.update({
            "ranges_json": json.dumps(ranges, sort_keys=True),
            "remote_file_bytes": remote_size,
            "local_file_bytes": local_path.stat().st_size,
            "status": "complete",
            "attempts": index_attempts + range_attempts,
            "starts_with_grib": integrity["starts_with_grib"],
            "ends_with_7777": integrity["ends_with_7777"],
            "message_count": integrity["message_count"],
            "metadata_checks_passed": integrity["metadata_checks_passed"],
            "completed_utc": iso_utc(datetime.now(timezone.utc)),
            })
        
        logger.info("Downloaded %s (%s bytes)", row["valid_time_utc"], row["local_file_bytes"])
    
    except Exception as error:
        row["status"] = "failed"
        row["error"] = repr(error)
        logger.exception("Failed %s", row["valid_time_utc"])
    
    return row


def main():
    """
    Process the configured valid-time range and persist progress continuously.

    """
    setup_logging()

    start = parse_utc(config.start_valid_time)
    end = parse_utc(config.end_valid_time)
    hours = list(valid_hours(start, end))

    # what if the files are downloaded partly, we can resume from those partly downloaded files
    manifest = load_manifest(config.manifest_path)
    logger.info("Processing %s valid hours from %s through %s", len(hours), iso_utc(start), iso_utc(end))

    session = requests.Session()
    session.headers.update({"User-Agent": config.user_agent})
    day_count = len({hour.date() for hour in hours})
    
    progress = tqdm(total=day_count, desc="Downloading HRRR", unit="day", dynamic_ncols=True)
    
    for position, valid_time in enumerate(hours, start=1):

        key = iso_utc(valid_time) # unique identifier for this valid time in the manifest
        logger.info("Hour %s/%s: %s", position, len(hours), key)
        manifest[key] = process_hour(session, valid_time, manifest.get(key))
        save_manifest(config.manifest_path, manifest)

        if position == len(hours) or hours[position].date() != valid_time.date():
            progress.update(1)

        if position < len(hours):
            time.sleep(config.pause_between_cycles_seconds)
    progress.close()

    complete = sum(row["status"] == "complete" for row in manifest.values() if row["valid_time_utc"] in {iso_utc(hour) for hour in hours})
    failed = len(hours) - complete
    logger.info("Configured range finished: %s complete, %s failed", complete, failed)
    
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

