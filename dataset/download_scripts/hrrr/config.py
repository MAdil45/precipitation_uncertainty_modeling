
"""
Configuration for selective HRRR historical downloads.
"""

from pathlib import Path


# resolve paths from this file so the script will work from any working directory.
project_root = Path(__file__).resolve().parents[3]
output_root = project_root / "dataset" / "raw_data" / "hrrr"
manifest_path = output_root / "download_manifest.csv"
log_path = output_root / "download.log"

# define the start and end time - feel free to change to a small test range first.
start_valid_time = "2025-05-01T00:00:00Z" # full historical range: 2021-01-01T00:00:00Z
end_valid_time   = "2025-12-31T05:00:00Z" # full historical range: 2025-12-31T23:00:00Z

# we will be using noaa HRRR archive to download the data
aws_base_url = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"
region = "conus"
product = "wrfsfc"
forecast_hour = 1

# these strings must match the NOAA HRRR .idx entries exactly. keeping the
# selectors explicit prevents a similarly named field or level being selected.
# for further details refer to: https://github.com/MAdil45/precipitation_uncertainty_modeling/blob/main/dataset/data_understanding/hrrr/hrrr_data_understanding.ipynb
selected_variables = {
                "apcp": {
                    "variable": "APCP",
                    "level": "surface",
                    "time_description": "0-1 hour acc fcst",
                    },
                "temperature_2m": {
                    "variable": "TMP",
                    "level": "2 m above ground",
                    "time_description": "1 hour fcst",
                    },
                "relative_humidity_2m": {
                    "variable": "RH",
                    "level": "2 m above ground",
                    "time_description": "1 hour fcst",
                    },
                    }

# lets also have network settings. each retry starts a new request and uses increasing delay.
connect_timeout_seconds = 30
read_timeout_seconds = 300
max_retries = 3
retry_backoff_seconds = 2
pause_between_cycles_seconds = 0.1

user_agent = "QPE-Bias-Correction-HRRR-Downloader/1.0"

