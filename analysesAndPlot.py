
# this script is used to analyse the different data from the consumers and producers
# it requests all interesting obiscodes for the different POD's and merges them into one dataframe
# it then calculates the total consumption and production for each obiscode
# and adds a final row with the total consumption and production
# it then saves the dataframe to a csv file


# import libraries

import yaml
import pandas as pd
import requests
import numpy as np
import matplotlib.pyplot as plt
import argparse
from date_generator import generate_date_range
from get_sun_data import get_sun_visibility
from add_weatherdata import process_data
import time
import csv
import re
from collections import defaultdict
import plotly.graph_objects as go
import plotly.io as pio


# define input parameters
parser = argparse.ArgumentParser(
    description='Generate ISO format datetime strings for month ranges with UTC timezone an get the data from that period from Leneda'
)


parser.add_argument(
    '-g', '--groupeDePartageNumber',
    type=str,
    required=True,
    help='What yaml file to use (example : CR00006041)'
)
parser.add_argument(
    '-y', '--year',
    type=int,
    required=True,
    help='Year (positive integer)'
)

parser.add_argument(
    '-m', '--month',
    type=int,
    required=True,
    help='Starting month (1-12)'
)

parser.add_argument(
    '-e', '--end-month',
    type=int,
    required=False,
    help='Optional ending month (1-12)'
)



args = parser.parse_args()

datapath = "/Users/marcdurbach/Development/python/ComEner-calculator/data"
configpath = "/Users/marcdurbach/Development/python/ComEner-calculator/configs"
yamlFileToUse = configpath+"/"+args.groupeDePartageNumber+".yaml"
groupeDePartage = args.groupeDePartageNumber
# generate timestring to prefix file names
ftimestr = time.strftime("%Y%m%d-%H")
# api to get sunrise and sunset
sunApi = "https://api.sunrisesunset.io/json?lat=49.819294&lng=6.274638&date=2024-07-01&time_format=24"
#generate start and end datetime strings 
starttime, endtime = generate_date_range(args.year, args.month, args.end_month)
print(f"Start datetime: {starttime}")
print(f"End datetime: {endtime}")
# read config file
csv_data = [["Nom", "Consommation", "a payer","Production","a recevoir"]]
with open(yamlFileToUse, 'r') as file:
    comener = yaml.safe_load(file)
# get consumers data from config file
consumers = comener['consumers']['names']
consumption = []
idx = 0
for consumer in consumers:
    consumption.append({"name":consumer,"meteringPoint":comener['consumers']['smartmeters'][idx],"obiscode":comener['consumers']['obiscode'][idx]})
    idx = idx+1
# get producers data from config file and load them in a json object
producers = comener['producers']['names']
production = []
idx = 0
for producer in producers:
    production.append({"name":producer,"meteringPoint":comener['producers']['smartmeters'][idx],"obiscode":comener['producers']['obiscode'][idx]})
    idx = idx+1
print("Consumer details")
for element in consumption:
    print(element)
print("Producer details")
for element in production:
    print(element)

# start data retrieval
# get data from producers


# This will hold all DataFrames keyed by name
dataframes = {}
# create obiscode list for producers
# production, autoconsommation, comm locsl, com nat, grid
obiscode_producers = ["1-1:2.29.0","1-65:2.29.1","1-65:2.29.2","1-65:2.29.3","1-65:2.29.9"]
#production (0), partagé avec soi-même (1), partagé par CEL (2), partagé_résidentiel (3)), vendu fournisseur(9)
obiscode_consumers = ["1-1:1.29.0","1-65:1.29.1","1-65:1.29.2","1-65:1.29.3","1-65:1.29.9"]
#consommation (0),autoconsommation_air (1), consommation par CEL (2), autoconsommation _résidentiel (3)), acheté fournisseur(9)
merged_df = None
for obiscode in obiscode_producers:
    print(obiscode)
    for producer in production:
        meterid = producer['meteringPoint']
        # create GET request for the Leneda Platform
        url = comener['leneda']['url']+comener['leneda']['api']['meteringData']+meterid+"/time-series?startDateTime="+starttime+"&endDateTime="+endtime+"&obisCode="+obiscode
        print("producer url : ",url)
        headers = {
        comener['leneda']['energyId']['header']: comener['leneda']['energyId']['value'],
        comener['leneda']['apiKey']['header']: comener['leneda']['apiKey']['value'],
        }
        df_name = f"df-prod-{producer['name']}-{obiscode}"   # dynamically create a name for the dataframe, based on the name of the person
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Make sure the request succeeded
        json_data = response.json() # store the result in a json object
        items = json_data.get('items', []) # get the items from the json object

        # Only process if there are items, the call may return a result, but no items, if the POD is not configured yet
        if not items:
            print(f"⚠️ No data returned for {producer['name']} with OBIS {obiscode}. Skipping.")
            continue

        df = pd.DataFrame(items)

        # Only keep 'startedAt' and 'value' if they exist
        if 'startedAt' not in df.columns or 'value' not in df.columns:
            print(f"⚠️ 'startedAt' or 'value' missing in data for {df_name}. Skipping.")
            continue

        df = df[['startedAt', 'value']].copy()
        df['startedAt'] = pd.to_datetime(df['startedAt'])
        df = df.rename(columns={'value': f'prod_{producer["name"]}_{obiscode}'})
        print(df)

        # add the dataframe to the dictionary with the name of the producer and the obiscode

        dataframes[df_name] = df

# get data from consumers
for obiscode in obiscode_consumers:
    print(obiscode)
    for consumer in consumption:
        meterid = consumer['meteringPoint']
        # create GET request for the Leneda Platform
        url = comener['leneda']['url']+comener['leneda']['api']['meteringData']+meterid+"/time-series?startDateTime="+starttime+"&endDateTime="+endtime+"&obisCode="+obiscode
        print("producer url : ",url)
        headers = {
        comener['leneda']['energyId']['header']: comener['leneda']['energyId']['value'],
        comener['leneda']['apiKey']['header']: comener['leneda']['apiKey']['value'],
        }
        df_name = f"df-cons-{consumer['name']}-{obiscode}"   # dynamically create a name for the dataframe, based on the name of the person
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Make sure the request succeeded
        json_data = response.json()
        items = json_data.get('items', [])

        # Only process if there is data
        if not items:
            print(f"⚠️ No data returned for {consumer['name']} with OBIS {obiscode}. Skipping.")
            continue

        df = pd.DataFrame(items)

        # Only keep 'startedAt' and 'value' if they exist
        if 'startedAt' not in df.columns or 'value' not in df.columns:
            print(f"⚠️ 'startedAt' or 'value' missing in data for {df_name}. Skipping.")
            continue

        df = df[['startedAt', 'value']].copy()
        df['startedAt'] = pd.to_datetime(df['startedAt'])
        df = df.rename(columns={'value': f'cons_{consumer["name"]}_{obiscode}'})
        print(df)

        # add the dataframe to the dictionary with the name of the consumer and the obiscode

        dataframes[df_name] = df
# at this moment, all dataframes are in the dictionary dataframes as received by the API queries

for df in dataframes.values():
    if merged_df is None:
        merged_df = df
    else:
        merged_df = pd.merge(merged_df, df, on='startedAt', how='outer')
# now the dataframes are merged into one dataframe, using the startedAt column as the key
# Sort the DataFrame by 'startedAt' column
merged_df['startedAt'] = pd.to_datetime(merged_df['startedAt'])
# Sort by timestamp (optional)
merged_df = merged_df.sort_values(by='startedAt')



merged_df.to_csv(f'{datapath}/{groupeDePartage}-analyses_plot.csv', index=False)
print("CSV file 'analyses.csv' has been created.")  
#
# # now we create some charts :
#    
# # Convert comma strings to floats

merged_df = merged_df.applymap(lambda x: float(str(x).replace(',', '.')) if isinstance(x, str) and ',' in str(x) else x)
# Set index to datetime
merged_df.index = pd.to_datetime(merged_df['startedAt'])
merged_df = merged_df.drop(columns=['startedAt'])  # Optional, if already in index
merged_df.to_csv(f'{datapath}/{yamlFileToUse}-analyses_new.csv', index=False)
# Helper function to extract parts
def parse_col(col):
    match = re.match(r"(prod|cons)_(.*?)_(\d.*)", col)
    if match:
        return match.groups()  # type, name, obis
    return None

# Group columns by type + OBIS
grouped_columns = defaultdict(list)

for col in df.columns:
    parsed = parse_col(col)
    if parsed:
        type_, name, obis = parsed
        key = f"{type_}_{obis}"
        grouped_columns[key].append(col)

# Helper function to extract parts
def parse_col(col):
    match = re.match(r"(prod|cons)_(.*?)_(\d.*)", col)
    if match:
        return match.groups()  # type, name, obis
    return None

# Group columns by type + OBIS
grouped_columns = defaultdict(list)

for col in merged_df.columns:
    parsed = parse_col(col)
    if parsed:
        type_, name, obis = parsed
        key = f"{type_}_{obis}"
        grouped_columns[key].append(col)

# New dataframe with grouped sums
grouped_df = pd.DataFrame(index=merged_df.index)

for key, cols in grouped_columns.items():
    grouped_df[key] = merged_df[cols].sum(axis=1)


# Separate consumption and production groups
cons_df = grouped_df.filter(like='cons')
prod_df = grouped_df.filter(like='prod')

# Plot lines
fig, ax = plt.subplots(figsize=(15, 6))
prod_df.plot.area(ax=ax, stacked=True, cmap='Greens', alpha=0.7)
cons_df.plot.area(ax=ax, stacked=True, cmap='Oranges', alpha=0.7)
ax.set_title("Production vs Consumption over Time")
ax.set_ylabel("Energy")
plt.tight_layout()
plt.show()

# plot bars
sample_range = slice(0, 96)  # 6 hours at 15 min intervals
x_ticks = grouped_df.index[sample_range].strftime('%H:%M')

# Plotting
fig, ax = plt.subplots(figsize=(18, 6))

# Stacked bar for production
bottom = None
for col in prod_df.columns:
    ax.bar(
        x_ticks,
        prod_df[col].iloc[sample_range],
        bottom=bottom,
        label=col,
        width=0.8
    )
    bottom = prod_df[col].iloc[sample_range] if bottom is None else bottom + prod_df[col].iloc[sample_range]

# Overlay consumption as another stacked bar (in red/orange shades)
bottom = None
for col in cons_df.columns:
    ax.bar(
        x_ticks,
        -cons_df[col].iloc[sample_range],  # Negative for downward bars
        bottom=bottom,
        label=col,
        width=0.8
    )
    bottom = -cons_df[col].iloc[sample_range] if bottom is None else bottom - cons_df[col].iloc[sample_range]

# Style
ax.set_title("Stacked Bar Plot: Production (up) vs Consumption (down)")
ax.set_ylabel("Energy (kWh)")
ax.set_xlabel("Time (HH:MM)")
ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.show()

# and here is a plotly version
# Filter prod and cons columns
prod_df = grouped_df.filter(like='prod')
cons_df = grouped_df.filter(like='cons')

# Select a smaller time window if needed (e.g., first 24 rows = 6 hours)
sample_range = slice(0, 24)
timestamps = grouped_df.index[sample_range]

# Create Plotly figure
fig = go.Figure()

# Add production columns (positive values)
for col in prod_df.columns:
    fig.add_trace(go.Bar(
        x=timestamps,
        y=prod_df[col].iloc[sample_range],
        name=f'Prod: {col}',
        marker_color='green'
    ))

# Add consumption columns (negative values for visual contrast)
for col in cons_df.columns:
    fig.add_trace(go.Bar(
        x=timestamps,
        y=-cons_df[col].iloc[sample_range],
        name=f'Cons: {col}',
        marker_color='red'
    ))

# Update layout for stacked bars
fig.update_layout(
    barmode='relative',
    title='Interactive Stacked Bar: Production (↑) vs Consumption (↓)',
    xaxis_title='Time',
    yaxis_title='Energy (kWh)',
    legend_title='Meters',
    xaxis_tickformat='%H:%M',
    height=600,
    template='plotly_white'
)
pio.renderers.default = "browser"  # Set browser as default renderer
fig.show()
fig.write_html("plot.html", auto_open=True)  # This opens it after saving