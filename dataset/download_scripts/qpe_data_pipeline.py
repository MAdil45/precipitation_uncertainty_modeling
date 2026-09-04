"""
QPE Bias Correction Project — Bulk Data Download Pipeline
============================================================

Downloads all three first-draft inputs over a date range:
  1. MRMS RadarOnly_QPE_01H   (CONUS, ~every 2 min)
  2. MRMS RadarAccumulationQualityIndex_01H (CONUS, hourly)
  3. MADIS gauge precipitation (Colorado Basin only, hourly), cleaned,
     deduplicated, spatially filtered, and saved as one Parquet file per day.

This mirrors the exact logic developed in the single-day sample notebooks
(radar_only_qpe.ipynb, raqi.ipynb, madis_gage_observations.ipynb) — same
parsing, same dedup priority order, same flag-don't-discard columns —
just looped over a date range and written to disk instead of displayed
inline.

All settings live in config.py. Edit that file, then run:
    python qpe_data_pipeline.py

Run for a single day first (start_date == end_date in config.py) to sanity
check paths and outputs before pointing this at a multi-year range on the
VACC.
"""

import gzip
import logging
import re
import shutil
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm

import boto3
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig

from config import Config

logger = logging.getLogger("qpe_pipeline")


# a few shared helpers
def setup_logging(cfg):
    
    cfg.log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(cfg.log_file),
            logging.StreamHandler(),
            ],
            )

def daterange(start_str, end_str):
    """Yield YYYYMMDD strings from start to end, inclusive."""
    start = datetime.strptime(start_str, "%Y%m%d")
    end = datetime.strptime(end_str, "%Y%m%d")
    n_days = (end - start).days
    if n_days < 0:
        raise ValueError(f"end_date {end_str} is before start_date {start_str}")
    for i in range(n_days + 1):
        yield (start + timedelta(days=i)).strftime("%Y%m%d")


# MRMS RadarOnly QPE + RAQI - shared download logic
def list_mrms_day_keys(bucket, product, date_str, s3_client):
    """List every .grib2.gz key for one product/day, paginating just in
    case a given day ever exceeds the 1000-key single-response limit."""
    prefix = f"{product}/{date_str}/"
    keys = []
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**kwargs)
        keys.extend(
            obj["Key"] for obj in response.get("Contents", [])
            if obj["Key"].endswith(".grib2.gz")
            )
        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break
    return keys


def download_mrms_day(bucket, product, date_str, compress_dir, extract_dir, s3_client):
    """Download + extract every file for one product/day. Skips files
    that already exist on disk, so re-running is cheap."""

    # ensuring if the directories exist
    compress_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    # now lets get the product keys for the whole day - it should have around 720 files - one file every 2 minutes
    keys = list_mrms_day_keys(bucket, product, date_str, s3_client)

    if not keys:
        logger.warning(f"[MRMS] {product} {date_str}: no files found on S3 - skipping")
        return {"found": 0, "downloaded": 0, "extracted": 0}

    # now lets loop through each key and get that file
    downloaded = 0
    extracted = 0
    for key in tqdm(keys, desc=f"{product} {date_str} files", leave=False):
        fname = Path(key).name
        year = fname.split("-")[0].split("_")[-1][:4]
        month = fname.split("-")[0].split("_")[-1][4:6]
        date_ = fname.split("-")[0].split("_")[-1][6:]

        # get the paths
        gz_path = compress_dir / year / month / date_ 
        gz_path.mkdir(parents=True, exist_ok=True)
        gz_file_path = gz_path / fname
        if not gz_file_path.exists():
            s3_client.download_file(bucket, key, str(gz_file_path))
            downloaded += 1
            
        # extract that file and move to the extraction directory
        grib_path = extract_dir / year / month / date_ 
        grib_path.mkdir(parents=True, exist_ok=True)
        grib_file_path = grib_path / fname.replace(".gz", "")
        if not grib_file_path.exists():
            with gzip.open(gz_file_path, "rb") as f_in, open(grib_file_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            extracted += 1

    logger.info(
        f"[MRMS] {product} {date_str}: {len(keys)} files found, "
        f"{downloaded} newly downloaded, {extracted} newly extracted"
        )
    return {"found": len(keys), "downloaded": downloaded, "extracted": extracted}


def run_mrms_pipeline(bucket, product, compress_dir, extract_dir, dates):
    s3_client = boto3.client("s3", config=BotoConfig(signature_version=UNSIGNED))
    for date_str in tqdm(dates, desc=f"Downloading {product}"):
        try:
            download_mrms_day(bucket, product, date_str, compress_dir, extract_dir, s3_client)
        except Exception as e:
            logger.error(f"[MRMS ERROR] {product} {date_str}: {e}\n{traceback.format_exc()}")


# MADIS gauges
def get_basin_geometry(shapefile_path, rfc_name):
    """Read the RFC shapefile and return (geometry, crs, bbox)."""
    
    shape_data = gpd.read_file(shapefile_path)
    basin = shape_data[shape_data["RFC_NAME"] == rfc_name].reset_index(drop=True) # filtering the RFC
    
    if basin.empty:
        raise ValueError(f"RFC_NAME '{rfc_name}' not found in shapefile {shapefile_path}")
        
    # get the bounds and return them alongside the geometry and CRS info
    lon_min, lat_min, lon_max, lat_max = basin.total_bounds
    
    return basin.geometry.iloc[0], basin.crs, (lat_min, lat_max, lon_min, lon_max)


def fetch_madis_hour(nominal_time, lat_min, lat_max, lon_min, lon_max, variables,
                      base_url, params_template, retries):
    """One hourly GET request, with retry + backoff. Returns the raw
    response text (or '' if every retry failed -- caller treats that
    like an empty/missing hour)."""

    # lets first add the Lats and Longs/BBOX to the params dictionary
    params = params_template.copy()
    params.update({
        "time": nominal_time,
        "latll": lat_min, "lonll": lon_min,
        "latur": lat_max, "lonur": lon_max,
        })

    # create the base url and add the params
    url = f"{base_url}?" + "&".join(f"{k}={v}" for k, v in params.items())
    url += "".join(f"&nvars={v}" for v in variables)

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=60, headers={"User-Agent": "research-script/1.0"})
            response.raise_for_status()
            return response.text
            
        except requests.RequestException as e:
            if attempt == retries - 1:
                logger.warning(f"[MADIS WARN] {nominal_time}: failed after {retries} attempts ({e})")
                return ""
                
            time.sleep(2 * (attempt + 1))
            
    return ""


def download_madis_day(date_str, lat_min, lat_max, lon_min, lon_max, variables,
                        base_url, params_template, sleep_between_requests, retries):
    """24 hourly requests for one day, kept in memory only -- no
    per-hour .txt files written to disk."""
    raw_texts = {}

    # get the list of 24 hours and we will loop for each hour
    hours = [f"{h:02d}00" for h in range(24)]
    for hour in tqdm(hours, desc='Looping Through Hours to Download Each hour data:'):
        nominal_time = f"{date_str}_{hour}"

        # call earlier defined fetch_madis_hour and add the output in the raw_texts with nomial_time as key
        raw_texts[nominal_time] = fetch_madis_hour(nominal_time, lat_min, lat_max, lon_min, lon_max, variables,
                                                    base_url, params_template, retries)

        # add the sleep to avoid sending too many requests
        time.sleep(sleep_between_requests)
        
    return raw_texts


def parse_madis_response(text, variables):
    """Extract the <pre>...</pre> CSV block. Case-insensitive match --
    at least one hourly file in testing used <PRE>, and a case-sensitive
    .find() silently corrupted station IDs and dropped rows."""
    match = re.search(r"<pre>(.*?)</pre>", text, re.IGNORECASE | re.DOTALL)
    if not match:
        return pd.DataFrame()

    body = match.group(1).strip()
    if not body or "No matching data found" in body:
        return pd.DataFrame()

    base_cols = ["station_id", "date", "time", "provider", "subprovider"]
    var_cols = []
    for v in variables:
        var_cols += [f"{v}_value", f"{v}_qcd", f"{v}_qca", f"{v}_qcr"]
    all_cols = base_cols + var_cols

    rows = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        fields = [f.strip() for f in line.split(",")]
        fields = fields[: len(all_cols)]
        if len(fields) == len(all_cols):
            rows.append(fields)

    return pd.DataFrame(rows, columns=all_cols)


def combine_precip(row, missing_value):
    """PCP1H wins when present; ASOS-HFM-only stations fall back to PCP1HA."""
    if row["PCP1H_value"] != missing_value:
        return row["PCP1H_value"], row["PCP1H_qcd"]
    elif row["PCP1HA_value"] != missing_value:
        return row["PCP1HA_value"], row["PCP1HA_qcd"]
    else:
        return np.nan, None


def minutes_from_hour(time_str):
    try:
        _, mm = time_str.split(":")
        mm = int(mm)
        return min(mm, 60 - mm)
    except Exception:
        return 999

def process_madis_day(raw_texts, variables, basin_geom, basin_crs, missing_value, qc_rank_map):
    """Parse -> numeric conversion -> spatial filter -> combine
    PCP1H/PCP1HA -> compute flag columns -> dedup.
    Returns a plain (non-geo) DataFrame ready to write to Parquet, or
    None if the day produced no usable rows at all."""

    # first lets create the dataframe for this day
    all_hours_df = []
    for nominal_time, text in raw_texts.items():
        df = parse_madis_response(text, variables)
        if not df.empty:
            df["nominal_time"] = nominal_time
            all_hours_df.append(df)

    if not all_hours_df:
        return None

    day_df = pd.concat(all_hours_df, ignore_index=True)

    # converting the numbers to numeric values
    value_cols = ["PCP1H_value", "PCP1HA_value", "LAT_value", "LON_value"]
    for col in value_cols:
        day_df[col] = pd.to_numeric(day_df[col], errors="coerce")

    # get the valid lats and longs to use for spatial filtering
    valid_latlon = (
        day_df["LAT_value"].notna() & day_df["LON_value"].notna()
        & (day_df["LAT_value"] != missing_value) & (day_df["LON_value"] != missing_value)
        )
    day_df_geo = day_df[valid_latlon].copy()
    if day_df_geo.empty: # this is highly unlikely
        return None

    # now lets create a geodataframe and perform the spatial filtering
    stations_gdf = gpd.GeoDataFrame(day_df_geo,
                        geometry=gpd.points_from_xy(day_df_geo["LON_value"], day_df_geo["LAT_value"]), crs=basin_crs)
    
    in_basin_df = stations_gdf[stations_gdf.within(basin_geom)].reset_index(drop=True)
    if in_basin_df.empty:
        return None

    basin_df = in_basin_df.copy()

    # lets combine PCP1H/PCP1HA into a single precipitation column and create a column for mm too
    basin_df[["precip_1h_m", "precip_1h_qcd"]] = basin_df.apply(lambda r: pd.Series(combine_precip(r, missing_value)), axis=1)
    basin_df["precip_1h_mm"] = basin_df["precip_1h_m"] * 1000

    # glag columns will be flag, we wont discard - all of these things are being considered 
    # after our earlier analysis - see MADIS notebook under data understanding section
    basin_df["minutes_from_hour"] = basin_df["time"].apply(minutes_from_hour)
    basin_df["time_window_uncertain"] = basin_df["provider"] == "ASOS-HFM"
    basin_df["is_uncertain_pathway"] = basin_df["time_window_uncertain"].astype(int)
    basin_df["has_value"] = basin_df["precip_1h_mm"].notna().astype(int)
    basin_df["qc_rank"] = basin_df["precip_1h_qcd"].map(qc_rank_map).fillna(-1)

  
    # at this stage we dont have a mechanism for outliers detection 
    # later will be added once the data is downloaded.
    basin_df["outlier_suspect"] = False

    # working on removing the duplicates - again the information are coming from earlier analysis 
    # see madis notebook under data understanding section for further details. 
    rank_cols = ["has_value", "is_uncertain_pathway", "minutes_from_hour", "qc_rank"]
    basin_df_sorted = basin_df.sort_values(by=["station_id", "nominal_time"] + rank_cols, 
                                           ascending=[True, True, False, True, True, False]).reset_index(drop=True)

    grp = basin_df_sorted.groupby(["station_id", "nominal_time"], sort=False)
    is_first_in_group = grp.cumcount() == 0
    next_row_vals = grp[rank_cols].shift(-1)
    same_as_next = (basin_df_sorted[rank_cols] == next_row_vals).all(axis=1)
    basin_df_sorted["tie_break_arbitrary"] = is_first_in_group & same_as_next

    basin_df_dedup = basin_df_sorted.drop_duplicates( subset=["station_id", "nominal_time"], keep="first").reset_index(drop=True)

    # lets drop geometry before saving - we will have plain parquet with numeric lats.longs, not geoparquet - no need for it now
    out_df = pd.DataFrame(basin_df_dedup.drop(columns="geometry"))
    return out_df


def save_madis_day_parquet(df, date_str, output_dir):
    """One Parquet file per day, Hive-style date partition so a
    filtered read (e.g. pyarrow.dataset / pd.read_parquet with
    filters) can skip whole days without touching them."""

    year, month, date_ = date_str[:4], date_str[4:6], date_str[6:]
    
    # creating the paths
    day_dir = output_dir / year / month 
    day_dir.mkdir(parents=True, exist_ok=True)
    
    out_path = day_dir / f"{date_}.parquet"
    print(out_path)
    df.to_parquet(out_path, index=False)
    
    return out_path


def run_madis_pipeline(cfg, dates, basin_geom, basin_crs, lat_min, lat_max, lon_min, lon_max):
    for date_str in tqdm(dates, desc="MADIS days"):
        try:
            # ensure that path exist
            year, month, date_ = date_str[:4], date_str[4:6], date_str[6:]
            out_path = cfg.madis_output_dir / year / month 
            out_path.mkdir(parents=True, exist_ok=True)
            out_file_path = out_path / f"{date_}.parquet"
    
            if out_file_path.exists():
                logger.info(f"[MADIS] {date_str}: parquet already exists - skipping")
                continue
    
            # else - lets download the file - first get the raw texts for this day - this will be 24 data points
            # for each station inside the provided aoi - lots of texts
            raw_texts = download_madis_day(
                            date_str, lat_min, lat_max, lon_min, lon_max, cfg.madis_variables,
                            cfg.madis_base_url, cfg.madis_params_template,
                            cfg.madis_sleep_between_requests, cfg.madis_retries,
                            )
    
            # now lets process it - filter the data, filter the duplicates etc. 
            day_df = process_madis_day(
                            raw_texts, cfg.madis_variables, basin_geom, basin_crs,
                            cfg.madis_missing, cfg.madis_qc_rank,
                            )
            
            if day_df is None or day_df.empty:
                logger.info(f"[MADIS] {date_str}: No Usable In-Basin Rows - Nothing Wsritten")
                continue
    
            # now lets save the data
            out_path = save_madis_day_parquet(day_df, date_str, cfg.madis_output_dir)
            logger.info(f"[MADIS] {date_str}: wrote {len(day_df)} rows -> {out_path}")

        except Exception as e:
            logger.error(f"[MADIS ERROR] {date_str}: {e}\n{traceback.format_exc()}")


# lets have our main method now
def main():
    cfg = Config()
    setup_logging(cfg)
     
    # get the dates
    dates = list(daterange(cfg.start_date, cfg.end_date))
    logger.info(f"Starting pipeline for {len(dates)} day(s): {cfg.start_date} -> {cfg.end_date}")

    # now lets run for each pipeline depending on which pipeline to download
    # RadarOnlyQPE
    if cfg.run_radar:
        
        logger.info("=== RadarOnly QPE ===")
        run_mrms_pipeline(cfg.radar_bucket, cfg.radar_product, cfg.radar_compress_dir, cfg.radar_extract_dir, dates)

    # RAQI
    if cfg.run_raqi:
        
        logger.info("=== RAQI ===")
        run_mrms_pipeline(cfg.raqi_bucket, cfg.raqi_product, cfg.raqi_compress_dir, cfg.raqi_extract_dir, dates)

    # MADIS Gages
    if cfg.run_madis:
        
        logger.info("=== MADIS gauges ===")
        
        # first lets get the BBOX and geometry information to filter out the gages which are not RFC specific
        basin_geom, basin_crs, (lat_min, lat_max, lon_min, lon_max) = get_basin_geometry(cfg.shapefile_path, cfg.rfc_name)
        logger.info(f"Basin bbox: lat [{lat_min:.3f}, {lat_max:.3f}], lon [{lon_min:.3f}, {lon_max:.3f}]")

        # now simply download the data for this RFC and for the required dates.
        run_madis_pipeline(cfg, dates, basin_geom, basin_crs, lat_min, lat_max, lon_min, lon_max)

    logger.info("Pipeline finished.")


if __name__ == "__main__":
    main()