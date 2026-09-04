
"""
config.py — central configuration for the QPE bulk data pipeline.

Edit the values below, then run:
    python qpe_data_pipeline.py

"""

from pathlib import Path


class Config:

    # first thing first - define start and end dates
    start_date = "20250401"
    end_date = "20250401"   # keep equal to start_date for the first test run

    # we have three pipelines: radar, raqi and madis and the script handle 
    # each pipeline individually - meaning we can control which to download
    run_radar = False
    run_raqi = False
    run_madis = True


    # shape file path - we will use to filter out the data to target a specific region
    shapefile_path = Path("/home/adil/Desktop/PhD_Research/Research_Projects/QPE_Project/"
                        "dataset/shapefiles/river_forecast_centers/rf05mr24.shp")
    rfc_name = "Colorado Basin"

    # define the parameters for each pipeline
    # MRMS - RadarOnlyQPE
    radar_bucket = "noaa-mrms-pds"
    radar_product = "CONUS/RadarOnly_QPE_01H_00.00"
    radar_compress_dir = Path("/home/adil/Desktop/PhD_Research/Research_Projects/QPE_Project/dataset/downloaded_data/radar_only_qpe/compressed")   
    radar_extract_dir = Path("/home/adil/Desktop/PhD_Research/Research_Projects/QPE_Project/dataset/downloaded_data/radar_only_qpe/extracted/")     

    # MRMS - RAQI
    raqi_bucket = "noaa-mrms-pds"
    raqi_product = "CONUS/RadarAccumulationQualityIndex_01H_00.00"
    raqi_compress_dir = Path("/home/adil/Desktop/PhD_Research/Research_Projects/QPE_Project/dataset/downloaded_data/radar_only_qpe/raqi/compressed")       
    raqi_extract_dir = Path("/home/adil/Desktop/PhD_Research/Research_Projects/QPE_Project/dataset/downloaded_data/radar_only_qpe/raqi/extracted")         

    # MADIS gauges
    madis_output_dir = Path("/home/adil/Desktop/PhD_Research/Research_Projects/QPE_Project/dataset/downloaded_data/madis/parquet")                  
    madis_base_url = "https://madis-data.ncep.noaa.gov/madisPublic/cgi-bin/madisXmlPublicDir"
    madis_variables = ["PCP1H", "PCP1HA", "LAT", "LON"]  # order matters for parsing
    madis_missing = -99999.0

    madis_params_template = {
        "rdr": "",
        "minbck": -59,
        "minfwd": 0,
        "recwin": 3,
        "timefilter": 0,
        "dfltrsel": 1,
        "state": "AK",   # unused leftover field, since we use lat/lon instead
        "stanam": "",
        "stasel": 0,
        "pvdrsel": 0,
        "varsel": 1,
        "qctype": 0,
        "qcsel": 0,       # keep everything, QC stays as a column
        "xml": 4,         # CSV with full QC
        "csvmiss": 0,      # -99999 for missing
        }

    madis_qc_rank = {"V": 3, "S": 2, "C": 1, "Q": 0, "Z": 0, "X": -1}

    madis_sleep_between_requests = 1.0   # seconds, polite pause between hourly requests
    madis_retries = 3

    # logging
    log_file = Path("/home/adil/Desktop/PhD_Research/Research_Projects/QPE_Project/dataset/download_scripts/pipeline_log.txt")                     