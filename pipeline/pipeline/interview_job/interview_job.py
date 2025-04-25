from dagster import job

from pipeline.interview_job.ops.add_gw_available_column import add_gw_available_column
from pipeline.interview_job.ops.get_mw_available_for_new_buses import get_mw_available_for_new_buses
from pipeline.interview_job.ops.raw_buses_to_run import raw_buses_to_run
from pipeline.interview_job.output.output_interview_job import output_interview_job


@job
def interview_job():
    df_raw_buses_to_run = raw_buses_to_run()

    df_buses_with_calculation = get_mw_available_for_new_buses(
        df_raw_buses_to_run
    )

    df_buses_with_calculation_transformed = add_gw_available_column(
        df_buses_with_calculation
    )

    output_interview_job(df_buses_with_calculation_transformed)
