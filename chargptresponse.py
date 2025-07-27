import pandas as pd
import requests

# List of (name, url) pairs for API calls
api_calls = [
    ("producer1", "https://api.example.com/data1"),
    ("producer2", "https://api.example.com/data2"),
    # add more as needed
]

# This will hold all DataFrames keyed by name
dataframes = {}

for name, url in api_calls:
    response = requests.get(url)
    response.raise_for_status()  # Make sure the request succeeded
    
    # Convert JSON response to DataFrame
    df = pd.DataFrame(response.json()['items'])
    df['startedAt'] = pd.to_datetime(df['startedAt'])
    
    # Rename value column to make it unique per API call
    df = df[['startedAt', 'value']].rename(columns={'value': f'value_{name}'})
    
    dataframes[name] = df

# Merge all DataFrames on 'startedAt'
# Start with the first one
merged_df = None
for df in dataframes.values():
    if merged_df is None:
        merged_df = df
    else:
        merged_df = pd.merge(merged_df, df, on='startedAt', how='outer')

# Sort by timestamp (optional)
merged_df = merged_df.sort_values(by='startedAt')

# Save to CSV
merged_df.to_csv('merged_output.csv', index=False)

print("CSV file 'merged_output.csv' has been created.")
