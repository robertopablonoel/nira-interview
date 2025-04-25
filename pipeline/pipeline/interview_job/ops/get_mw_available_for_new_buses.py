import os
import pandas as pd
from dagster import get_dagster_logger, op

from pipeline.interview_job.ops.get_mw_available_for_each_bus_very_slow import (
    _run_slow_calculation_for_bus,
)


@op
def get_mw_available_for_new_buses(df_bus_numbers):
    output_dir = "pipeline/interview_job/output"
    
    # Check for existing output files
    existing_files = [f for f in os.listdir(output_dir) if f.startswith("processed_buses_") and f.endswith(".csv")]
    if existing_files:
        # Get the most recent file based on timestamp in filename
        latest_file = max(existing_files)
        existing_df = pd.read_csv(os.path.join(output_dir, latest_file))
        
        # Find which buses we need to calculate
        existing_buses = set(existing_df["bus_number"])
        new_buses = df_bus_numbers[~df_bus_numbers["bus_number"].isin(existing_buses)]
        
        if not new_buses.empty:
            # Only calculate for new buses
            new_buses["mw_available"] = new_buses.apply(
                lambda row: _run_slow_calculation_for_bus(row["bus_number"]), axis=1
            )
            result_df = pd.concat([existing_df, new_buses])
        else:
            result_df = existing_df
    else:
        # Calculate for all buses
        df_bus_numbers["mw_available"] = df_bus_numbers.apply(
            lambda row: _run_slow_calculation_for_bus(row["bus_number"]), axis=1
        )
        result_df = df_bus_numbers
    
    return result_df 