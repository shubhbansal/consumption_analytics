import pandas as pd

stock_products_final = pd.read_csv('./product_2.csv')
stock_suppliers_final = pd.read_csv('./supplier_1.csv')

# TODO: Mark products produced locally (AV, bio-region)
# TODO: Send list of suppliers without city

print 'AV products count: ', len(stock_products_final[stock_products_final['is_av_product'] == 'Y'])
# Finding suppliers which supply Auroville products
stock_suppliers_final['is_av_supplier'] = 'N'
stock_suppliers_final.loc[stock_suppliers_final['supplier_city'].isnull(), 'supplier_city'] = 'NA'
stock_suppliers_final.loc[
    stock_suppliers_final['supplier_city'].str.contains('auroville', case=False), 'is_av_supplier'] = 'Y'

print len(stock_suppliers_final[stock_suppliers_final['is_av_supplier'] == 'Y'])
extra_non_av_suppliers = ['PT Purchasing', 'Gentlemen Farming', 'Wellwin Organic', 'Nizan from Iran', 'Veerasami - Samira candles', 'Wild Yeast Bakery', 'SANDEEP']
stock_suppliers_final.loc[stock_suppliers_final['supplier_name'].isin(extra_non_av_suppliers), 'is_av_supplier'] = 'N'
print len(stock_suppliers_final[stock_suppliers_final['is_av_supplier'] == 'Y'])

stock_av_suppliers = stock_suppliers_final[stock_suppliers_final['is_av_supplier'] == 'Y']
stock_non_av_suppliers = stock_suppliers_final[stock_suppliers_final['is_av_supplier'] != 'Y']

# Find products where supplier is AV supplier. Ignore supplier2 and supplier 3.
# stock_av_suppliers_products = pd.merge(stock_av_suppliers, stock_products_final, on= ['supplier_id'])

stock_products_final['is_av_corrected'] = 'N'
stock_products_final.loc[stock_products_final['supplier_id'].isin(stock_av_suppliers['supplier_id']), 'is_av_corrected'] = 'Y'
print 'AV products corrected: ', len(stock_products_final[stock_products_final['is_av_corrected'] == 'Y'])

# This is the code for checking supplier2 and supplier3 which is not needed right now.
# stock_av_suppliers_products_2 = pd.merge(stock_av_suppliers, stock_products_final, left_on= ['supplier_id'], right_on= ['supplier2_id']).sort_index(axis=1)
# stock_av_suppliers_products_3 = pd.merge(stock_av_suppliers, stock_products_final, left_on= ['supplier_id'], right_on= ['supplier3_id']).sort_index(axis=1)
#
# stock_av_suppliers_products = stock_av_suppliers_products_1.append(stock_av_suppliers_products_2.append(stock_av_suppliers_products_3))
# stock_av_suppliers_av_products = stock_av_suppliers_products[stock_av_suppliers_products['is_av_corrected'] == 'Y']
# stock_av_suppliers_products = stock_av_suppliers_products.groupby(['supplier_id', 'supplier_name', 'supplier_city']).agg({'product_id': 'count'}).reset_index()
# stock_av_suppliers_av_products = stock_av_suppliers_av_products.groupby(['supplier_id', 'supplier_name', 'supplier_city']).agg({'is_av_corrected': 'count'}).reset_index()

# stock_av_suppliers_products_count = pd.merge(stock_av_suppliers_products, stock_av_suppliers_av_products, how= 'left', on= ['supplier_id', 'supplier_name', 'supplier_city'])

#Find products where AV lies in product_description or category. Mark them as AV products.
# 22 more products got added.
stock_products_final['product_description_1'] = ' ' + stock_products_final['product_description'] + ' '
stock_products_final['category_description_1'] = ' ' + stock_products_final['category_description'] + ' '

stock_products_final.loc[
    stock_products_final['product_description_1'].str.contains(' AV ', case=False), 'is_av_corrected'] = 'Y'
stock_products_final.loc[
    stock_products_final['category_description_1'].str.contains(' AV ', case=False), 'is_av_corrected'] = 'Y'
print 'AV products corrected: ', len(stock_products_final[stock_products_final['is_av_corrected'] == 'Y'])

print stock_products_final.columns

drop_columns = ['Unnamed: 0', 'Unnamed: 0_x', 'Unnamed: 0_y', 'is_perishable_y', 'is_modified', 'product_description_1',
                 'category_description_1']
stock_products_final = stock_products_final.drop(columns=drop_columns)
stock_products_final.to_csv('product_3.csv')
stock_suppliers_final.to_csv('supplier_2.csv')

# stock_av_suppliers_products_count.to_csv('av_suppliers_data.csv')
# print len(stock_av_suppliers)
# print len(stock_av_suppliers_products)
# print len(stock_av_suppliers_products_count)




# print stock_av_suppliers_products.columns.tolist()
#
# stock_av_suppliers_products.to_csv('av_suppliers.csv')



# stock_products_final.loc[(stock_products_final['supplier_id'] == 2) | (
#     stock_products_final['supplier2_id'] == 2) | (stock_products_final['supplier3_id'] == 2), 'is_av_corrected_1'] = 'N'





# stock_products_final.to_csv('products_final_av_updated.csv')

# print len(stock_products_final[stock_products_final['is_av_corrected'] != stock_products_final['is_av_corrected_1']])
# print len(stock_products_final[stock_products_final['is_av_product'] != stock_products_final['is_av_corrected_1']])
# print len(stock_products_final[
#               (stock_products_final['is_av_corrected'] == 'Y') & (stock_products_final['is_av_corrected_1'] == 'N')])
# print len(stock_products_final[
#               (stock_products_final['is_av_corrected'] == 'N') & (stock_products_final['is_av_corrected_1'] == 'Y')])
