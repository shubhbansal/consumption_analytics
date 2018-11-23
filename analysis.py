import pandas as pd

# Stock_products: ['Unnamed: 0', 'product_id', 'product_code', 'product_description', 'is_av_product', 'is_perishable_x', 'measurement_unit_id', 'supplier_id', 'supplier2_id', 'supplier3_id', 'category_id', 'start_year', 'product_uid', 'category_code', 'category_description', 'parent_category_id', 'Flag', 'is_av_corrected']
# Bill_Items_Data: ['Unnamed: 0', 'Unnamed: 0.1', 'bill_item_id', 'quantity', 'price', 'product_id', 'bill_id', 'bill_uid', 'bill_item_uid', 'product_uid', 'product_description', 'Items_flag', 'product_uid_corrected']

# stock_products = pd.read_csv('./product_3.csv')
# bill_items_data = pd.read_csv('./bill_items_2.csv')
# bill_data = pd.read_csv('./bill.csv')
#
# stock_products_columns = ['product_uid', 'product_description', 'category_id', 'category_description', 'is_av_corrected']
# stock_products = stock_products[stock_products_columns]
#
# bill_items_data_columns = ['bill_item_uid', 'quantity', 'price', 'bill_uid', 'product_description', 'product_uid_corrected']
# bill_items_data = bill_items_data[bill_items_data_columns]
# bill_items_data = bill_items_data.rename(columns= {'product_uid_corrected': 'product_uid'})
# bill_items_data['amount'] = bill_items_data['quantity'] * bill_items_data['price']
#
# bill_columns = ['bill_uid', 'date_created', 'account_number', 'account_name']
# bill_data = bill_data[bill_columns]
# bill_data['date_created'] = pd.to_datetime(bill_data['date_created'])
# bill_data['year'] = bill_data['date_created'].dt.year
#
# print len(bill_items_data)
# bill_items_data = pd.merge(bill_items_data, bill_data, on= ['bill_uid'])
# bill_items_data = pd.merge(bill_items_data, stock_products, on= ['product_uid', 'product_description'])
# print len(bill_items_data)
#
# # All bill_items_av are in a separate table and all bill_items_non_av are in a separate table
# bill_items_data_av = bill_items_data[bill_items_data['is_av_corrected'] == 'Y']
# bill_items_data_non_av = bill_items_data[bill_items_data['is_av_corrected'] == 'N']
#
# print len(bill_items_data_av) + len(bill_items_data_non_av)
#
# # Saving it separatelty to minimise time for the future. You can run the code in parts by commenting out above part and then reading from the csv.
# bill_items_data_av.to_csv('bill_items_data_av.csv')
# bill_items_data_non_av.to_csv('bill_items_data_non_av.csv')

bill_items_data_av = pd.read_csv('./bill_items_data_av.csv')
bill_items_data_non_av = pd.read_csv('./bill_items_data_non_av.csv')

#Year-wise summary will have annual AV, Non-AV consumption analysis
year_wise_av = bill_items_data_av.groupby(['year']).agg({'amount': 'sum'}).reset_index()
year_wise_av = year_wise_av.rename(columns= {'amount': 'Amount AV'})

year_wise_non_av = bill_items_data_non_av.groupby(['year']).agg({'amount': 'sum'}).reset_index()
year_wise_non_av = year_wise_non_av.rename(columns= {'amount': 'Amount Non AV'})

year_wise_summary = pd.merge(year_wise_av, year_wise_non_av, on= ['year'])
year_wise_summary['Total Amount'] = year_wise_summary['Amount AV'] + year_wise_summary['Amount Non AV']
year_wise_summary['AV %'] = year_wise_summary['Amount AV'] * 100 / year_wise_summary['Total Amount']

# print year_wise_summary

year_wise_summary.to_csv('year_wise_summary.csv')

# year_category_wise_analysis finds AV and non-AV consumption for each year-category combination
year_category_wise_av = bill_items_data_av.groupby(['year', 'category_id', 'category_description']).agg({'amount': 'sum'}).reset_index()
year_category_wise_av = year_category_wise_av.rename(columns= {'amount': 'Amount AV'})

year_category_wise_non_av = bill_items_data_non_av.groupby(['year', 'category_id', 'category_description']).agg({'amount': 'sum'}).reset_index()
year_category_wise_non_av = year_category_wise_non_av.rename(columns= {'amount': 'Amount Non AV'})

# Outer join is done to ensure no row is being missed out.
year_category_wise_analysis = pd.merge(year_category_wise_av, year_category_wise_non_av, how= 'outer', on= ['year', 'category_id', 'category_description'])
year_category_wise_analysis.loc[year_category_wise_analysis['Amount AV'].isnull(), 'Amount AV'] = 0
year_category_wise_analysis.loc[year_category_wise_analysis['Amount Non AV'].isnull(), 'Amount Non AV'] = 0
year_category_wise_analysis.to_csv('year_category_wise_analysis.csv')

year_account_wise_av = bill_items_data_av.groupby(['year', 'account_number', 'account_name']).agg({'amount': 'sum'}).reset_index()
year_account_wise_av = year_account_wise_av.rename(columns= {'amount': 'Amount AV'})

year_account_wise_non_av = bill_items_data_non_av.groupby(['year','account_number', 'account_name']).agg({'amount': 'sum'}).reset_index()
year_account_wise_non_av = year_account_wise_non_av.rename(columns= {'amount': 'Amount Non AV'})

year_account_wise_analysis = pd.merge(year_account_wise_av, year_account_wise_non_av, how= 'outer', on= ['year', 'account_number', 'account_name'])
year_account_wise_analysis.loc[year_account_wise_analysis['Amount AV'].isnull(), 'Amount AV'] = 0
year_account_wise_analysis.loc[year_account_wise_analysis['Amount Non AV'].isnull(), 'Amount Non AV'] = 0
year_account_wise_analysis.to_csv('year_account_wise_analysis.csv')

print 'Phew!'