from datetime import datetime
from dagster import op


@op
def output_interview_job(context, df_with_all_columns):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_path = f"pipeline/interview_job/output/processed_buses_{timestamp}.csv"
    df_with_all_columns.to_csv(
        output_file_path, index=False, line_terminator="\n", encoding="utf-8"
    )
