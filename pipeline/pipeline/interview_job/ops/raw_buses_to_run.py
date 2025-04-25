import pandas as pd
from dagster import op

#_RAW_CSV_TO_READ = "pipeline/interview_job/raw/raw_buses.csv"
_RAW_CSV_TO_READ = "pipeline/interview_job/raw/new_raw_buses.csv"


@op
def raw_buses_to_run():
    return pd.read_csv(
        _RAW_CSV_TO_READ,
        index_col=None,
        encoding="utf-8",
    )
