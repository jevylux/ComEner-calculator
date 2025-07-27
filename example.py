import pandas as pd
from typing import List, Dict
import json

def process_energy_datasets(json_files: List[Dict]):
    """
    Process multiple energy consumption datasets and calculate percentages of total.
    
    Parameters:
    json_files (List[Dict]): List of dictionaries containing the JSON data
    
    Returns:
    Dict[str, pd.DataFrame]: Dictionary of individual processed DataFrames with percentages
    pd.DataFrame: Summary DataFrame with values and percentages
    """
    dataframes = {}
    all_data = []
    
    # Process each dataset
    for data in json_files:
        name = data.get('name', 'default')
        df_name = f"df-{name}"
        
        # Convert the dataset to DataFrame
        df = pd.DataFrame(data['data'])
        
        # Parse the nested dictionary in the last column
        df['parsed_values'] = df.iloc[:, -1].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        
        # Extract values and timestamps into separate columns
        df['value'] = df['parsed_values'].apply(lambda x: x['value'])
        df['startedAt'] = df['parsed_values'].apply(lambda x: pd.to_datetime(x['startedAt']))
        
        # Store the processed DataFrame
        dataframes[df_name] = df
        
        # Prepare data for summary
        summary_data = {
            'startedAt': df['startedAt'],
            'value': df['value'],
            'source': df_name
        }
        all_data.append(pd.DataFrame(summary_data))
    
    # Combine all data into a single DataFrame
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Calculate total sum for each timestamp
    timestamp_sums = combined_df.groupby('startedAt')['value'].sum().reset_index()
    timestamp_sums.columns = ['startedAt', 'total_sum']
    
    # Add total sums back to combined DataFrame
    combined_df = combined_df.merge(timestamp_sums, on='startedAt')
    
    # Calculate percentages
    combined_df['percentage'] = (combined_df['value'] / combined_df['total_sum'] * 100).round(2)
    
    # Update individual dataframes with percentages
    for df_name in dataframes.keys():
        mask = combined_df['source'] == df_name
        df_with_pct = combined_df[mask].copy()
        dataframes[df_name] = dataframes[df_name].assign(
            percentage=df_with_pct['percentage'].values,
            total_sum=df_with_pct['total_sum'].values
        )
    
    # Create summary with percentages by source
    summary = combined_df.pivot_table(
        index='startedAt',
        columns='source',
        values=['value', 'percentage'],
        aggfunc='first'
    ).reset_index()
    
    # Add total sums to summary
    summary['total_sum'] = timestamp_sums['total_sum']
    
    return dataframes, summary

# Example usage:
"""
# Process the datasets
dataframes, summary = process_energy_datasets([data1, data2])

# Access individual DataFrame with percentages
df1 = dataframes['df-dataset1']
print(f"Value: {df1['value'][0]}")
print(f"Percentage of total: {df1['percentage'][0]}%")
print(f"Total sum at this timestamp: {df1['total_sum'][0]}")

# Access summary for all datasets
print(summary.head())
"""