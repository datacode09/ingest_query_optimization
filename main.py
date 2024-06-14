# Import necessary modules
import os
import sys
import pyodbc
import pandas as pd
import time
import traceback
from datetime import datetime, timedelta
from Monitoring_Logs import *  # Assuming Monitoring_Logs has necessary functions
from sendEmail import sendErrorEmail  # Assuming sendEmail has necessary functions
from Teradata_Connection import connect_to_sql  # Assuming you have a function for Teradata connection

# Record start time
start = datetime.now()

# Define monitoring variables
task_name = os.getenv("taskName")
shell_name = os.getenv("shellName")

# Define environment variables they get their value from shell script
query = os.getenv("query")
query_src = os.getenv("query_src")
data_src = os.getenv("data_src")
import_src = os.getenv("import_src")
file_str_1 = os.getenv("file_str_1")
file_str_2 = os.getenv("file_str_2")

task_id = get_taskid(shell_name)

# List of date ranges (replace with your actual date ranges)
date_ranges = [
    ('2023-01-01', '2023-01-01'),
    ('2023-02-01', '2023-02-01'),
    # Add more date ranges as needed
]

# Batch size
batch_size = 1000  # Adjust the batch size as needed
# Delay in seconds between executions
delay_seconds = 10

# Query template with ROW_NUMBER for pagination
query_template = """
WITH PaginatedDecisions AS (
    SELECT DISTINCT
        DEC.DecisionID,
        DEC.ApplicationID,
        DEC.DealerCode,
        CONVERT(datetime, SWITCHOFFSET(DEC.SubmissionDate, DATEPART(TZOFFSET, DEC.SubmissionDate AT TIME ZONE 'Eastern Standard Time'))) AS SubmissionDate,
        CONVERT(datetime, SWITCHOFFSET(DEC.DecisionDate, DATEPART(TZOFFSET, DEC.DecisionDate AT TIME ZONE 'Eastern Standard Time'))) AS DecisionDate,
        DEC.CreditDecision,
        DEC.CreditAnalyst,
        DEC.AmountToFinance,
        DEC.Vehicle_Make,
        CRD.LTV,
        CRD.CombinedTDSR,
        CRD.Authorization,
        CRD.APPProgramType,
        DI.RequestedOn,
        ROW_NUMBER() OVER (ORDER BY DEC.DecisionID) AS RowNum
    FROM EDM.dbo.Decisions AS DEC WITH (NOLOCK)
    LEFT JOIN EDM.CAF.DTInterface AS DI WITH (NOLOCK)
    ON CRD.ApplicationID = DEC.ApplicationID
    AND DEC.SubmissionDate = DI.RequestedOn
    WHERE DEC.ApplicationID IN (
        SELECT ApplicationID
        FROM EDM.dbo.Decisions WITH (NOLOCK)
        WHERE CAST(CONVERT(datetime, SWITCHOFFSET(DecisionDate, DATEPART(TZOFFSET, DecisionDate AT TIME ZONE 'Eastern Standard Time'))) AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    )
)
SELECT * FROM PaginatedDecisions
WHERE RowNum BETWEEN {start_row} AND {end_row};
"""

try:
    # Connect to SQL Server
    SQL_SERVER_CONN = connect_to_sql()
    SQL_SERVER_CURSOR = SQL_SERVER_CONN.cursor()

    for start_date, end_date in date_ranges:
        start_row = 1
        end_row = batch_size

        while True:
            try:
                # Execute the query
                SQL_SERVER_CURSOR.execute(query_template.format(
                    start_date=start_date,
                    end_date=end_date,
                    start_row=start_row,
                    end_row=end_row)
                )
                
                # Fetch the results into a pandas DataFrame
                columns = [column[0] for column in SQL_SERVER_CURSOR.description]
                rows = SQL_SERVER_CURSOR.fetchall()
                df = pd.DataFrame.from_records(rows, columns=columns)
                
                if df.empty:
                    break  # Exit the loop if no more data is returned

                # Localize datetime columns
                for column in df.select_dtypes(include=['datetime64[ns]']).columns:
                    df[column] = df[column].dt.tz_localize('America/Toronto', ambiguous='infer', nonexistent='shift_forward')

                # Save DataFrame to Parquet
                df.to_parquet(f'{data_src}/{query}_{file_str_1}_{start_date}_{end_date}_rows_{start_row}_to_{end_row}.parquet')

                # Update row numbers for the next batch
                start_row += batch_size
                end_row += batch_size

            except Exception as e:
                print(f"Error processing rows {start_row} to {end_row} for date range {start_date} to {end_date}: {e}")
                sendErrorEmail(task_id, task_name, start, e)  # Send an error email

            # Delay between executions
            time.sleep(delay_seconds)

    # Close the connection
    SQL_SERVER_CURSOR.close()
    SQL_SERVER_CONN.close()

except Exception as e:
    print(e)
    traceback.print_exc()
    sendErrorEmail(task_id, task_name, start, e)  # Send an error email
