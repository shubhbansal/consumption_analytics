# TODO: Generate list of all products which are not supplied by AV units but marked as AV
# TODO: Generate list of all products which are supplied by AV units and not marked as is_AV
# TODO: Generate list of product descriptions with different product ids

#TODO: Remove rows where product_description matches with description in product table

import pandas as pd

stock_products_merged = pd.read_csv('./product_1.csv')
stock_categories_merged = pd.read_csv('./category.csv')

# stock_products_merged = pd.read_hdf('./ptdc_merged_data.h5', 'product')
# stock_categories_merged = pd.read_hdf('./ptdc_merged_data.h5', 'category')

# print len(stock_products_merged)To check if all products have valid category_id
stock_products_merged = pd.merge(stock_products_merged, stock_categories_merged, on=['category_id'])
# print len(stock_products_merged)

stock_products_merged['Flag'] = 'NA'

# find all rows in stock_products where product_description is unique
unique_stock_products = stock_products_merged.groupby(['product_description']).agg(
    {'product_id': 'count'}).reset_index()
unique_stock_products = unique_stock_products[unique_stock_products['product_id'] == 1]

# Add flag 'Y' to product rows with unique product_description, 'N' with 'xxx', 'FREE CODE' & 'Not Used'
stock_products_merged.loc[stock_products_merged.product_description.isin(unique_stock_products.product_description), 'Flag'] = 'Y'
stock_products_merged.loc[stock_products_merged.product_description.str.startswith('xxx'), 'Flag'] = 'N'
stock_products_merged.loc[stock_products_merged.product_description.str.startswith('XXX'), 'Flag'] = 'N'
stock_products_merged.loc[stock_products_merged['product_description'] == 'FREE CODE', 'Flag'] = 'N'
stock_products_merged.loc[stock_products_merged['product_description'] == 'NOT USED', 'Flag'] = 'N'

print len(stock_products_merged[stock_products_merged['Flag'] == 'N']), len(
    stock_products_merged[stock_products_merged['Flag'] == 'Y']), len(
    stock_products_merged[stock_products_merged['Flag'] == 'NA'])

# In the remaining rows, find rows where product id, code and description are same but categories are different.
# Add flag 'Y' to the row with the latest year or largest uid in this table as it will have the correct category.
# Add flag 'N' to other rows.
# TODO: Update product_uid in product_desc for these rows.
stock_products_merged_unflagged = stock_products_merged[stock_products_merged['Flag'] == 'NA']

stock_products_merged_unflagged = stock_products_merged_unflagged[
    stock_products_merged_unflagged.duplicated(subset=['product_id', 'product_code', 'product_description'],
                                               keep=False)]
stock_products_merged_unflagged = stock_products_merged_unflagged.sort_values(by=['product_uid'])
stock_products_merged_unflagged_y = stock_products_merged_unflagged.drop_duplicates(subset=['product_id', 'product_code', 'product_description'], keep='last')
stock_products_merged_unflagged_n = stock_products_merged_unflagged[
    ~stock_products_merged_unflagged.isin(stock_products_merged_unflagged_y)].dropna(how='all')

stock_products_merged.loc[stock_products_merged['product_uid'].isin(stock_products_merged_unflagged_y['product_uid']), 'Flag'] = 'Y'
stock_products_merged.loc[stock_products_merged['product_uid'].isin(stock_products_merged_unflagged_n['product_uid']), 'Flag'] = 'N'

# One case that is an outlier is changed.
stock_products_merged.loc[(stock_products_merged['product_description'] == 'EGG AV MEDIUM') & (
    stock_products_merged['category_id'] == 16), 'Flag'] = 'N'
stock_products_merged.loc[(stock_products_merged['product_description'] == 'EGG AV MEDIUM') & (
    stock_products_merged['category_id'] == 5), 'Flag'] = 'Y'

# Total count of Y, N, NA is equal to total count of products. All good till here.
print len(stock_products_merged[stock_products_merged['Flag'] == 'N']), len(
    stock_products_merged[stock_products_merged['Flag'] == 'Y']), len(
    stock_products_merged[stock_products_merged['Flag'] == 'NA'])

# We can keep rows where product_desc and category_desc are same, because difference in product_ids doesn't matter much.
stock_products_merged_unflagged = stock_products_merged[stock_products_merged['Flag'] == 'NA']
stock_products_merged_unflagged_y = stock_products_merged_unflagged[
    stock_products_merged_unflagged.duplicated(subset=['product_description', 'category_description'], keep=False)]
stock_products_merged.loc[stock_products_merged.index.isin(stock_products_merged_unflagged_y.index), 'Flag'] = 'Y'

print len(stock_products_merged[stock_products_merged['Flag'] == 'N']), len(
    stock_products_merged[stock_products_merged['Flag'] == 'Y']), len(
    stock_products_merged[stock_products_merged['Flag'] == 'NA'])

# Only 15 products are left and there doesn't seem to be any problems in them.
stock_products_merged_unflagged = stock_products_merged[stock_products_merged['Flag'] == 'NA']
stock_products_merged.loc[stock_products_merged['Flag'] == 'NA','Flag'] = 'Y'

stock_products_final = stock_products_merged[stock_products_merged['Flag'] == 'Y']
stock_products_final.to_csv('product_2.csv')
# stock_products_filtered = stock_products_merged[stock_products_merged['Flag'] == 'Y']
#
# stock_products_filtered.to_csv('products_cleaned_171118.csv')
# stock_products_merged_unflagged.to_csv('unflagged_products_2.csv')
# print stock_products_merged[stock_products_merged['Flag'] == 'N']

# Removing single duplicate entry that was still left.
# stock_product_data = stock_product_data[
#     (stock_product_data.product_id != 2356) | (stock_product_data.product_code != '559')]
# print stock_product_data[stock_product_data['product_id'] == 2356]

# duplicate_products = stock_product_data[stock_product_data.duplicated(subset='product_id',keep=False)]
# duplicate_products.to_csv('duplicate_products.csv')
