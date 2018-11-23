import pandas as pd

stock_products_final = pd.read_csv('./product_3.csv')
bill_items_data = pd.read_csv('./bill_items_1.csv')

# For unique product descriptions, compare product_description directly

# If product id and description matches, then update product_uid

print 'Total bill items: ', len(bill_items_data)
# Delete rows with 'xxx', 'FREE CODE', 'NOT USED'
bill_items_data['Items_flag'] = 'NA'
bill_items_data.loc[bill_items_data['product_description'].isin(['FREE CODE', 'NOT USED']), 'Items_flag'] = 'N'
bill_items_data.loc[bill_items_data.product_description.str.startswith('XXX'), 'Items_flag'] = 'N'
bill_items_data.loc[bill_items_data.product_description.str.startswith('xxx'), 'Items_flag'] = 'N'

print 'Deleted rows: ', len(bill_items_data[bill_items_data['Items_flag'] == 'N'])

stock_products_final = stock_products_final[['product_id', 'product_description', 'product_uid']]
bill_items_data_unfiltered = bill_items_data[bill_items_data['Items_flag']== 'NA']

# Left join to ensure no row is missed.
bill_items_data_unfiltered = pd.merge(bill_items_data_unfiltered, stock_products_final, how= 'left', on= ['product_id', 'product_description'])
bill_items_data_unfiltered.loc[bill_items_data_unfiltered['product_uid_y'].notna(), 'Items_flag'] = 'Y'
bill_items_data_unfiltered_y = bill_items_data_unfiltered[bill_items_data_unfiltered['Items_flag'] == 'Y']
bill_items_data_unfiltered_na = bill_items_data_unfiltered[bill_items_data_unfiltered['Items_flag'] == 'NA']
bill_items_data_unfiltered_y = bill_items_data_unfiltered_y[['bill_item_uid', 'product_uid_y']]
bill_items_data_unfiltered_y = bill_items_data_unfiltered_y.rename(columns={'product_uid_y': 'product_uid_corrected'})

bill_items_data_corrected = bill_items_data[bill_items_data['Items_flag'] != 'N']
print 'Remaining rows: ', len(bill_items_data_corrected)

# To get correct product_uid in bill_items_data_corrected.
bill_items_data_corrected = pd.merge(bill_items_data_corrected, bill_items_data_unfiltered_y, how= 'left', on=['bill_item_uid'])
bill_items_data_corrected.loc[bill_items_data_corrected['product_uid_corrected'].notna(), 'Items_flag'] = 'Y'
print 'Rows with correct product_uid: ', len(bill_items_data_corrected[bill_items_data_corrected['Items_flag'] == 'Y'])
print 'Rows which needs correct product_uid: ', len(bill_items_data_unfiltered_na)

bill_items_data_corrected.to_csv('bill_items_2.csv')

# stock_products_with_unique_id_descriptions = stock_products_final.groupby(['product_description']).agg({'product_code': 'count'}).reset_index()
# print len(stock_products_with_unique_id_descriptions)

# stock_products_with_unique_id_descriptions = stock_products_with_unique_id_descriptions[
#     stock_products_with_unique_id_descriptions['product_code'] == 1]
# stock_products_with_unique_id_descriptions = stock_products_with_unique_id_descriptions.drop(columns='product_code')
# stock_products_with_unique_id_descriptions['unique_id+desc'] = 'Y'
# print len(stock_products_with_unique_id_descriptions)
# # print stock_products_with_unique_id_descriptions.head()
#
# stock_products_final = pd.merge(stock_products_final, stock_products_with_unique_id_descriptions, how='left',
#                                 on=['product_id', 'product_description'])
# stock_products_final.loc[stock_products_final['unique_id+desc'].isnull(), 'unique_id+desc'] = 'NA'
# print len(stock_products_final), len(stock_products_final[stock_products_final['unique_id+desc'] == 'NA'])
#
# stock_products_with_non_unique_id_descriptions = stock_products_final[stock_products_final['unique_id+desc'] == 'NA']
# stock_products_with_non_unique_id_descriptions = stock_products_with_non_unique_id_descriptions.sort_values(
#     by=['product_uid'])
# stock_products_with_non_unique_id_descriptions_y = stock_products_with_non_unique_id_descriptions.drop_duplicates(
#     subset=['product_id', 'product_code', 'product_description'],
#     keep='last')
#
# stock_products_with_non_unique_id_descriptions_n = stock_products_with_non_unique_id_descriptions[
# ~stock_products_with_non_unique_id_descriptions.isin(stock_products_with_non_unique_id_descriptions_y)].dropna(
#     how='all')
#
# stock_products_with_non_unique_id_descriptions.loc[stock_products_with_non_unique_id_descriptions['product_uid'].isin(stock_products_with_non_unique_id_descriptions_y['product_uid']), 'unique_id+desc'] = 'Y'
# stock_products_with_non_unique_id_descriptions.loc[stock_products_with_non_unique_id_descriptions['product_uid'].isin(stock_products_with_non_unique_id_descriptions_n['product_uid']), 'unique_id+desc'] = 'N'
# # stock_products_with_non_unique_id_descriptions_y.to_csv('non_unique_products_y.csv')
#
# print len(stock_products_with_non_unique_id_descriptions[stock_products_with_non_unique_id_descriptions['unique_id+desc'] == 'NA'])
# # stock_products_with_non_unique_id_descriptions.to_csv('non_unique_products.csv')
#
# # For same product id and description, the remaining non-unique products either have changes in suplier details or category (only 2).
# # Picking the row with the latest product_uid
#
# # bill_items_data['Flag'] = 'NA'
# # bill_items_data.loc[bill_items_data['product_description'].isin(stock_products_with_unique_id_descriptions['product_description']), 'Flag'] = 'Y'
