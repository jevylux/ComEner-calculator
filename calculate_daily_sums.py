import yaml
import pandas as pd
import requests
import numpy as np
import argparse
from date_generator import generate_date_range
from get_sun_data import get_sun_visibility

# import data:
# Read the CSV file
df = pd.read_csv("merged_withsun.csv")

# View the first 5 rows
df.head()
daylight_cons_sum = df[df['sun_visible'] == True]['total_sum_cons'].sum() / 4
daylight_prod_sum = df[df['sun_visible'] == True]['total_sum_prod'].sum() / 4
ratio = (daylight_prod_sum/daylight_cons_sum) * 100
print(f"Considering the consumption during daylight({daylight_cons_sum:.2f}), and the production {daylight_prod_sum:.2f} , we have a ratio of {ratio:.2f}%")
night_cons_sum = df[df['sun_visible'] == False]['total_sum_cons'].sum() / 4
night_prod_sum = df[df['sun_visible'] == False]['total_sum_prod'].sum() / 4
ratio = (night_prod_sum/night_cons_sum) * 100
print(f"Considering the consumption during night({night_cons_sum:.2f}), and the production {night_prod_sum:.2f} , we have a ratio of {ratio:.2f}%")
total_cons_sum = df['total_sum_cons'].sum() / 4
total_prod_sum = df['total_sum_prod'].sum() / 4
ratio = (total_prod_sum/total_cons_sum) * 100
print(f"Total consumption ({total_cons_sum:.2f}), and total production {total_prod_sum:.2f} , we have a ratio of {ratio:.2f}%")
