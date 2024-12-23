import pandas as pd
from pandas_gbq import to_gbq

# Replace with your actual project ID and dataset.table name
project_id = 'your-project-id'
table_id = 'your_dataset.your_table'

# Sample DataFrame
data = {
    'column1': [1, 2, 3],
    'column2': ['A', 'B', 'C']
}
df = pd.DataFrame(data)

# Upload DataFrame to BigQuery
to_gbq(df, table_id, project_id=project_id, if_exists='replace')

