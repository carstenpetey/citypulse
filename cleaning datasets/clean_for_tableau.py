"""
cleaning data for tableau visualizations
need to make zip code level summary for 3 maps:
1. eviction rate change (aug 2023 vs aug 2025)
2. cash sale ratio (2024)
3. median sale price change (2022 vs 2024)

output: zip_summary_for_tableau.csv (no nulls)
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# load all the data files
print("loading data files...")
evictions = pd.read_csv('evictions_queried.csv', low_memory=False)
acris_master = pd.read_csv('acris-master_queried.csv', low_memory=False)
acris_legals = pd.read_csv('acris-legals_queried.csv', low_memory=False)
pluto = pd.read_csv('pluto_queried.csv', low_memory=False)
population = pd.read_csv('population_queried.csv', low_memory=False)

# need to create BBL from borough, block, lot in acris legals
acris_legals['bbl'] = (
    acris_legals['BOROUGH'].astype(str).str.strip() + 
    acris_legals['BLOCK'].astype(str).str.strip().str.zfill(5) + 
    acris_legals['LOT'].astype(str).str.strip().str.zfill(4)
)

# make sure pluto BBL is formatted correctly (10 digits)
pluto['bbl'] = pluto['BBL'].astype(str).str.strip().str.zfill(10)

# join acris master to legals to get BBL
acris_full = acris_master.merge(
    acris_legals[['DOCUMENT ID', 'bbl']], 
    on='DOCUMENT ID', 
    how='inner'
)

# then join to pluto to get zip codes
# remove duplicates so we don't multiply rows
pluto_unique = pluto[['bbl', 'postcode', 'landuse']].drop_duplicates(subset=['bbl'])
acris_with_zip = acris_full.merge(
    pluto_unique, 
    on='bbl', 
    how='left'
)

# clean up zip codes - make them 5 digits, remove invalid ones
acris_with_zip['zipcode'] = acris_with_zip['postcode'].astype(str).str.strip()
acris_with_zip['zipcode'] = acris_with_zip['zipcode'].str.replace(r'\.0+$', '', regex=True)
acris_with_zip['zipcode'] = acris_with_zip['zipcode'].replace(['nan', 'None', '', '0', '00000'], np.nan)
acris_with_zip['zipcode'] = acris_with_zip['zipcode'].where(
    acris_with_zip['zipcode'].str.match(r'^\d{5}$', na=False),
    np.nan
)
acris_with_zip['zipcode'] = acris_with_zip['zipcode'].str.zfill(5)

# drop records without zip codes
acris_with_zip = acris_with_zip[acris_with_zip['zipcode'].notna()].copy()

# viz 1: eviction rate change map (aug 2023 vs aug 2025)
# filter to residential only
evictions_res = evictions[evictions['Residential/Commercial'] == 'Residential'].copy()

# parse dates
evictions_res['executed_date'] = pd.to_datetime(evictions_res['Executed Date'], format='%m/%d/%Y', errors='coerce')
evictions_res = evictions_res[evictions_res['executed_date'].notna()].copy()

# clean zip codes
evictions_res['zipcode'] = evictions_res['Eviction Postcode'].astype(str).str.strip()
evictions_res['zipcode'] = evictions_res['zipcode'].str.replace(r'\.0+$', '', regex=True)
evictions_res['zipcode'] = evictions_res['zipcode'].replace(['nan', 'None', '', '0', '00000'], np.nan)
evictions_res['zipcode'] = evictions_res['zipcode'].where(
    evictions_res['zipcode'].str.match(r'^\d{5}$', na=False),
    np.nan
)
evictions_res['zipcode'] = evictions_res['zipcode'].str.zfill(5)
evictions_res = evictions_res[evictions_res['zipcode'].notna()].copy()

# get august 2023 and 2025
evictions_res['year'] = evictions_res['executed_date'].dt.year
evictions_res['month'] = evictions_res['executed_date'].dt.month

evictions_aug_2023 = evictions_res[(evictions_res['year'] == 2023) & (evictions_res['month'] == 8)].copy()
evictions_aug_2025 = evictions_res[(evictions_res['year'] == 2025) & (evictions_res['month'] == 8)].copy()

# count by zip
evictions_2023_by_zip = evictions_aug_2023.groupby('zipcode').size().reset_index(name='eviction_count_2023')
evictions_2025_by_zip = evictions_aug_2025.groupby('zipcode').size().reset_index(name='eviction_count_2025')

# clean population data
population['zipcode'] = population['MODZCTA'].astype(str).str.strip()
population['zipcode'] = population['zipcode'].str.replace(r'\.0+$', '', regex=True)
population['zipcode'] = population['zipcode'].replace(['nan', 'None', '', '0', '00000'], np.nan)
population['zipcode'] = population['zipcode'].where(
    population['zipcode'].str.match(r'^\d{5}$', na=False),
    np.nan
)
population['zipcode'] = population['zipcode'].str.zfill(5)
population['pop_est'] = population['pop_est'].astype(str).str.replace(',', '').astype(float)
population = population[population['zipcode'].notna()].copy()

# merge everything together
all_eviction_zips = set(evictions_2023_by_zip['zipcode'].tolist() + evictions_2025_by_zip['zipcode'].tolist())
viz1_data = pd.DataFrame({'zipcode': sorted(all_eviction_zips)})
viz1_data = viz1_data.merge(evictions_2023_by_zip, on='zipcode', how='left')
viz1_data = viz1_data.merge(evictions_2025_by_zip, on='zipcode', how='left')
viz1_data = viz1_data.merge(population[['zipcode', 'pop_est']], on='zipcode', how='left')

# fill missing values
viz1_data['eviction_count_2023'] = viz1_data['eviction_count_2023'].fillna(0)
viz1_data['eviction_count_2025'] = viz1_data['eviction_count_2025'].fillna(0)
viz1_data['pop_est'] = viz1_data['pop_est'].fillna(1)  # avoid division by zero

# calculate rates per 10k residents
viz1_data['eviction_rate_2023'] = (viz1_data['eviction_count_2023'] / viz1_data['pop_est']) * 10000
viz1_data['eviction_rate_2025'] = (viz1_data['eviction_count_2025'] / viz1_data['pop_est']) * 10000
viz1_data['eviction_rate_change'] = viz1_data['eviction_rate_2025'] - viz1_data['eviction_rate_2023']
viz1_data['eviction_rate_pct_change'] = np.where(
    viz1_data['eviction_rate_2023'] > 0,
    ((viz1_data['eviction_rate_2025'] - viz1_data['eviction_rate_2023']) / viz1_data['eviction_rate_2023']) * 100,
    0
)

# viz 2: cash sale ratio map (2024)
# parse dates
acris_with_zip['document_date'] = pd.to_datetime(acris_with_zip['DOC. DATE'], format='%m/%d/%Y', errors='coerce')
acris_with_zip = acris_with_zip[acris_with_zip['document_date'].notna()].copy()

# filter to 2024, DEED and MTGE only
acris_2024 = acris_with_zip[acris_with_zip['document_date'].dt.year == 2024].copy()
acris_2024 = acris_2024[acris_2024['DOC. TYPE'].isin(['DEED', 'MTGE'])].copy()

# filter to residential (landuse 1,2,3,4)
acris_2024['landuse'] = pd.to_numeric(acris_2024['landuse'], errors='coerce')
acris_2024_res = acris_2024[acris_2024['landuse'].isin([1, 2, 3, 4])].copy()

# separate deeds and mortgages
deeds_2024 = acris_2024_res[acris_2024_res['DOC. TYPE'] == 'DEED'].copy()
mtges_2024 = acris_2024_res[acris_2024_res['DOC. TYPE'] == 'MTGE'].copy()

# match deeds to mortgages - if a deed has a mortgage on same BBL within 90 days, it's financed
# otherwise it's a cash sale
deeds_merge = deeds_2024[['bbl', 'document_date', 'DOCUMENT ID', 'zipcode']].copy()
deeds_merge.columns = ['bbl', 'deed_date', 'document_id', 'zipcode']
mtges_merge = mtges_2024[['bbl', 'document_date']].copy()
mtges_merge.columns = ['bbl', 'mtge_date']

merged = deeds_merge.merge(mtges_merge, on='bbl', how='left', suffixes=('', '_mtge'))
merged['date_diff_days'] = (merged['mtge_date'] - merged['deed_date']).abs().dt.days
merged['has_matching_mtge'] = (merged['date_diff_days'] <= 90) & (merged['date_diff_days'].notna())

# for each deed, check if ANY mortgage matches
deed_matches = merged.groupby('document_id')['has_matching_mtge'].max().reset_index()
deed_matches['is_cash_sale'] = ~deed_matches['has_matching_mtge']

# merge back
deeds_2024 = deeds_2024.merge(deed_matches[['document_id', 'is_cash_sale']], 
                               left_on='DOCUMENT ID', right_on='document_id', how='left')
deeds_2024['is_cash_sale'] = deeds_2024['is_cash_sale'].fillna(True)  # default to cash if no match
deeds_2024 = deeds_2024.drop('document_id', axis=1)

# clean zip codes
deeds_2024['zipcode'] = deeds_2024['zipcode'].astype(str).str.strip()
deeds_2024['zipcode'] = deeds_2024['zipcode'].str.replace(r'\.0+$', '', regex=True)
deeds_2024['zipcode'] = deeds_2024['zipcode'].replace(['nan', 'None', '', '0', '00000'], np.nan)
deeds_2024['zipcode'] = deeds_2024['zipcode'].where(
    deeds_2024['zipcode'].str.match(r'^\d{5}$', na=False),
    np.nan
)
deeds_2024['zipcode'] = deeds_2024['zipcode'].str.zfill(5)
deeds_2024 = deeds_2024[deeds_2024['zipcode'].notna()].copy()

# aggregate by zip
viz2_data = deeds_2024.groupby('zipcode').agg({
    'DOCUMENT ID': 'count',
    'is_cash_sale': 'sum'
}).reset_index()
viz2_data.columns = ['zipcode', 'total_sales', 'cash_sales']
viz2_data['cash_sale_ratio'] = viz2_data['cash_sales'] / viz2_data['total_sales']

# only keep zips with at least 10 sales
viz2_data = viz2_data[viz2_data['total_sales'] >= 10].copy()

# viz 3: median sale price change map (2022 vs 2024)
# filter to DEED only, residential
acris_deeds = acris_with_zip[acris_with_zip['DOC. TYPE'] == 'DEED'].copy()
acris_deeds['landuse'] = pd.to_numeric(acris_deeds['landuse'], errors='coerce')
acris_deeds_res = acris_deeds[acris_deeds['landuse'].isin([1, 2, 3, 4])].copy()

# parse dates and amounts
acris_deeds_res['document_date'] = pd.to_datetime(acris_deeds_res['DOC. DATE'], format='%m/%d/%Y', errors='coerce')
acris_deeds_res = acris_deeds_res[acris_deeds_res['document_date'].notna()].copy()
acris_deeds_res['document_amt'] = pd.to_numeric(acris_deeds_res['DOC. AMOUNT'], errors='coerce')
acris_deeds_res = acris_deeds_res[acris_deeds_res['document_amt'] > 0].copy()  # remove zero amounts

# filter to 2022 and 2024
deeds_2022 = acris_deeds_res[acris_deeds_res['document_date'].dt.year == 2022].copy()
deeds_2024 = acris_deeds_res[acris_deeds_res['document_date'].dt.year == 2024].copy()

# clean zip codes
deeds_2022['zipcode'] = deeds_2022['zipcode'].astype(str).str.strip()
deeds_2022['zipcode'] = deeds_2022['zipcode'].str.replace(r'\.0+$', '', regex=True)
deeds_2022['zipcode'] = deeds_2022['zipcode'].replace(['nan', 'None', '', '0', '00000'], np.nan)
deeds_2022['zipcode'] = deeds_2022['zipcode'].where(
    deeds_2022['zipcode'].str.match(r'^\d{5}$', na=False),
    np.nan
)
deeds_2022['zipcode'] = deeds_2022['zipcode'].str.zfill(5)
deeds_2022 = deeds_2022[deeds_2022['zipcode'].notna()].copy()

deeds_2024['zipcode'] = deeds_2024['zipcode'].astype(str).str.strip()
deeds_2024['zipcode'] = deeds_2024['zipcode'].str.replace(r'\.0+$', '', regex=True)
deeds_2024['zipcode'] = deeds_2024['zipcode'].replace(['nan', 'None', '', '0', '00000'], np.nan)
deeds_2024['zipcode'] = deeds_2024['zipcode'].where(
    deeds_2024['zipcode'].str.match(r'^\d{5}$', na=False),
    np.nan
)
deeds_2024['zipcode'] = deeds_2024['zipcode'].str.zfill(5)
deeds_2024 = deeds_2024[deeds_2024['zipcode'].notna()].copy()

# calculate median prices by zip
median_2022 = deeds_2022.groupby('zipcode')['document_amt'].median().reset_index(name='median_price_2022')
median_2024 = deeds_2024.groupby('zipcode')['document_amt'].median().reset_index(name='median_price_2024')

# count sales per zip
count_2022 = deeds_2022.groupby('zipcode').size().reset_index(name='sales_count_2022')
count_2024 = deeds_2024.groupby('zipcode').size().reset_index(name='sales_count_2024')

# merge everything
viz3_data = median_2022.merge(median_2024, on='zipcode', how='outer')
viz3_data = viz3_data.merge(count_2022, on='zipcode', how='left')
viz3_data = viz3_data.merge(count_2024, on='zipcode', how='left')

# fill missing counts
viz3_data['sales_count_2022'] = viz3_data['sales_count_2022'].fillna(0)
viz3_data['sales_count_2024'] = viz3_data['sales_count_2024'].fillna(0)

# only keep zips with at least 5 sales in each period
viz3_data = viz3_data[
    (viz3_data['sales_count_2022'] >= 5) & 
    (viz3_data['sales_count_2024'] >= 5)
].copy()

# calculate price changes
viz3_data['price_change_dollars'] = viz3_data['median_price_2024'] - viz3_data['median_price_2022']
viz3_data['price_change_pct'] = np.where(
    viz3_data['median_price_2022'] > 0,
    ((viz3_data['median_price_2024'] - viz3_data['median_price_2022']) / viz3_data['median_price_2022']) * 100,
    0
)

# combine all three visualizations into one dataframe
# get all unique zip codes
all_zips = set()
if len(viz1_data) > 0:
    all_zips.update(viz1_data['zipcode'].unique())
if len(viz2_data) > 0:
    all_zips.update(viz2_data['zipcode'].unique())
if len(viz3_data) > 0:
    all_zips.update(viz3_data['zipcode'].unique())

# clean up zip codes
all_zips_cleaned = set()
for zipcode in all_zips:
    zip_str = str(zipcode).strip()
    zip_str = zip_str.replace('.0', '').replace('.', '')
    if zip_str and zip_str != 'nan' and zip_str != 'None' and zip_str != '0' and zip_str != '00000':
        if zip_str.isdigit() and len(zip_str) <= 5:
            zip_str = zip_str.zfill(5)
            if len(zip_str) == 5:
                all_zips_cleaned.add(zip_str)

# start with all cleaned zips
final_df = pd.DataFrame({'zipcode': sorted(all_zips_cleaned)})
final_df['zipcode'] = final_df['zipcode'].astype(str)

# merge viz 1 data
if len(viz1_data) > 0:
    viz1_data['zipcode'] = viz1_data['zipcode'].astype(str).str.strip()
    viz1_data['zipcode'] = viz1_data['zipcode'].str.replace(r'\.0+$', '', regex=True)
    viz1_data['zipcode'] = viz1_data['zipcode'].str.zfill(5)
    viz1_data = viz1_data[viz1_data['zipcode'].str.match(r'^\d{5}$', na=False)].copy()
    
    viz1_cols = ['zipcode', 'eviction_rate_2023', 'eviction_rate_2025', 
                 'eviction_rate_change', 'eviction_rate_pct_change']
    final_df = final_df.merge(viz1_data[viz1_cols], on='zipcode', how='left')

# merge viz 2 data
if len(viz2_data) > 0:
    viz2_data['zipcode'] = viz2_data['zipcode'].astype(str).str.strip()
    viz2_data['zipcode'] = viz2_data['zipcode'].str.replace(r'\.0+$', '', regex=True)
    viz2_data['zipcode'] = viz2_data['zipcode'].str.zfill(5)
    viz2_data = viz2_data[viz2_data['zipcode'].str.match(r'^\d{5}$', na=False)].copy()
    
    viz2_cols = ['zipcode', 'total_sales', 'cash_sales', 'cash_sale_ratio']
    final_df = final_df.merge(viz2_data[viz2_cols], on='zipcode', how='left')

# merge viz 3 data
if len(viz3_data) > 0:
    viz3_data['zipcode'] = viz3_data['zipcode'].astype(str).str.strip()
    viz3_data['zipcode'] = viz3_data['zipcode'].str.replace(r'\.0+$', '', regex=True)
    viz3_data['zipcode'] = viz3_data['zipcode'].str.zfill(5)
    viz3_data = viz3_data[viz3_data['zipcode'].str.match(r'^\d{5}$', na=False)].copy()
    
    viz3_cols = ['zipcode', 'median_price_2022', 'median_price_2024', 
                 'price_change_dollars', 'price_change_pct']
    final_df = final_df.merge(viz3_data[viz3_cols], on='zipcode', how='left')

# fill all nulls with 0
final_df = final_df.fillna(0)

# round to 2 decimal places
numeric_cols = final_df.select_dtypes(include=[np.number]).columns
for col in numeric_cols:
    final_df[col] = final_df[col].round(2)

# final cleanup of zip codes
final_df['zipcode'] = final_df['zipcode'].astype(str).str.strip()
final_df['zipcode'] = final_df['zipcode'].str.replace(r'\.0+$', '', regex=True)
final_df['zipcode'] = final_df['zipcode'].str.zfill(5)
final_df = final_df[final_df['zipcode'].str.match(r'^\d{5}$', na=False)].copy()
final_df = final_df.drop_duplicates(subset=['zipcode'], keep='first').copy()

# save to csv
output_file = 'zip_summary_for_tableau.csv'
final_df.to_csv(output_file, index=False)
print(f"done! saved to {output_file}")

