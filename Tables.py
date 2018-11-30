# PURPOSE: This file extracts data from multiple PTDC databases and their tables and merges them.
# sqlalchemy is the library used for performing read and write operations with MySql

import pandas as pd
import numpy as np
import sqlalchemy

# This function reads bill_items table from a given database and append it to bill_items_data dataframe after adding bill_uid, bill_item_uid and product_uid as unique columns.
# All bill_item are unique without any duplicacy.
bill_item_columns = ['bill_item_id', 'quantity', 'adjusted_quantity', 'price', 'product_id', 'bill_id',
                     'product_description']


def join_bill_items(bill_items_data, stock_product_year, year, month, engine):
    data = pd.read_sql('bill_items_201' + str(year) + '_' + str(month), engine,
                       columns=bill_item_columns)
    # print len(data) To check whether data is empty or not
    # Adding unique ids to bill and bill_items so that they could be identified after merging different databases
    data['bill_uid'] = (10000000 * year + 100000 * month + data['bill_id']).astype(np.uint32)
    data['bill_item_uid'] = (100000000 * year + 1000000 * month + data['bill_item_id']).astype(np.uint32)

    # Adding unique ids to products so that they could be linked to right product after merging different databases
    # DB stores data of a financial year (Apr-Mar). So for Jan-Mar, year to be be added in product_uid is year-1
    if month in range(1,4):
        data['product_uid'] = (10000 * (year-1) + data['product_id']).astype(np.uint32)
    else:
        data['product_uid'] = (10000 * year + data['product_id']).astype(np.uint32)

    data = change_bill_items_type(data)

    # TODO: Check whether these changes in product descriptions are justified or not.
    data = change_product_description_in_bill_item(data, stock_product_year)
    # print len(data) To check whether dataset size changes or not. Ideally, it shouldn't but some 120 rows are missing from 2015 because they deleted those products.

    data.loc[data['quantity'] == 0, 'quantity'] = data['adjusted_quantity']
    data = data.drop(columns=['adjusted_quantity'])
    bill_items_data = bill_items_data.append(data, ignore_index=True)
    return bill_items_data


# Converts columns of bill_items to most efficient data types as per their size.
# The data types for id related columns might have to be changed later if length of the table increases.
# We can also do it for other dataframes if we wish to speed up the execution time.
def change_bill_items_type(bill_items_data):
    bill_items_data['bill_id'] = bill_items_data['bill_id'].astype(np.uint16)
    # bill_items_data['bill_uid'] = bill_items_data['bill_uid'].astype(np.uint64)
    bill_items_data['bill_item_id'] = bill_items_data['bill_item_id'].astype(np.uint32)
    # bill_items_data['bill_item_uid'] = bill_items_data['bill_item_uid'].astype(np.uint64)
    bill_items_data['quantity'] = bill_items_data['quantity'].astype(np.float32)
    bill_items_data['adjusted_quantity'] = bill_items_data['adjusted_quantity'].astype(np.float32)
    bill_items_data['price'] = bill_items_data['price'].astype(np.float32)
    bill_items_data['product_id'] = bill_items_data['product_id'].astype(np.uint16)
    # bill_items_data['product_description'] = bill_items_data['product_description'].astype()
    return bill_items_data


def change_product_description_in_bill_item(bill_items_data, stock_product_year):
    # print len(bill_items_data), len(stock_product_year)
    stock_product_year = stock_product_year[['product_uid', 'product_description']]
    l1 = len(bill_items_data)
    # 120 rows in total will get lost because their corresponding product_ids got deleted.
    bill_items_data = pd.merge(bill_items_data, stock_product_year, on= ['product_uid'])
    bill_items_data = bill_items_data.drop(columns= 'product_description_x')
    bill_items_data = bill_items_data.rename(columns={'product_description_y': 'product_description'})
    l2 = len(bill_items_data)
    # if l1 != l2:
    #     print l1, l2, l1-l2
    return bill_items_data


# This function reads bill table from a given database and appends it to bill_data dataframe after adding bill_uid as unique column.
# All rows are unique without any duplicacy.
bill_columns = ['bill_id', 'bill_number', 'date_created', 'total_amount', 'user_id', 'account_number', 'account_name']


def join_bill(bill_data, year, month, engine):
    data = pd.read_sql('bill_201' + str(year) + '_' + str(month), engine, columns=bill_columns)
    data['bill_uid'] = 10000000 * year + 100000 * month + data['bill_id']
    bill_data = bill_data.append(data, ignore_index=True)
    return bill_data


# This function appends stock_product from a given database to stock_product_data dataframe and removes duplicates
product_columns = ['product_id', 'product_code', 'product_description', 'is_av_product', 'is_perishable',
                   'measurement_unit_id', 'supplier_id', 'supplier2_id', 'supplier3_id', 'category_id']

def get_stock_product_for_year(year, engine ):
    data = pd.read_sql('stock_product', engine, columns=product_columns)
    data['start_year'] = year
    data['product_uid'] = (10000 * year + data['product_id']).astype(np.uint32)
    return data

def join_stock_product(stock_product_data, stock_product_year):
    stock_product_data = stock_product_data.append(stock_product_year)
    duplicate_check_col = [x for x in stock_product_data.columns if x not in ['start_year', 'product_uid']]
    stock_product_data.drop_duplicates(subset=duplicate_check_col, keep='last', inplace=True)
    return stock_product_data


# This function appends stock_category from a given database to stock_category_data dataframe and removes duplicates
# Categories are unique on category_id and category_code
def join_stock_category(stock_category_data, engine):
    data = pd.read_sql('stock_category', engine)
    stock_category_data = stock_category_data.append(data)
    stock_category_data.drop_duplicates(subset=['category_id', 'category_code'], keep='last', inplace=True)
    return stock_category_data


# This function appends stock_supplier from a given database to stock_supplier_data dataframe and removes duplicates
# Stock_suppliers are unique on supplier_id
supplier_columns = ['supplier_id', 'supplier_code', 'supplier_name', 'contact_person', 'supplier_address',
                    'supplier_city', 'supplier_state']


def join_stock_supplier(stock_supplier_data, engine):
    data = pd.read_sql('stock_supplier', engine,
                       columns=supplier_columns)
    stock_supplier_data = stock_supplier_data.append(data)
    stock_supplier_data.drop_duplicates(subset=['supplier_id'], keep='last', inplace=True)
    return stock_supplier_data

stock_category_data = pd.DataFrame()
stock_supplier_data = pd.DataFrame()
stock_product_data = pd.DataFrame()
bill_data = pd.DataFrame()
bill_items_data = pd.DataFrame()

# To read all databaases from year 2013-18
for year in range(3, 9):
    year_begin = year
    year_end = year + 1

    # Because 2018 DB follows a different format.
    if year > 7:
        database_name = 'invent'
        month_begin_1 = 4
        month_end_1 = 11
        month_begin_2 = 1
        month_end_2 = 1
    else:
        database_name = 'invent_201' + str(year_begin) + '_201' + str(year_end)
        month_begin_1 = 4
        month_end_1 = 13
        month_begin_2 = 1
        month_end_2 = 4
    engine = sqlalchemy.create_engine('mysql://root:root@localhost:3306/' + database_name)
    stock_product_year = get_stock_product_for_year(year, engine)
    # Because DB has tables from April-March
    for month in range(month_begin_1, month_end_1):
        print year, month
        bill_data = join_bill(bill_data, year_begin, month, engine)
        bill_items_data = join_bill_items(bill_items_data, stock_product_year, year_begin, month, engine)
    for month in range(month_begin_2, month_end_2):
        print year, month
        bill_data = join_bill(bill_data, year_end, month, engine)
        bill_items_data = join_bill_items(bill_items_data, stock_product_year, year_end, month, engine)

    stock_product_data = join_stock_product(stock_product_data, stock_product_year)
    stock_category_data = join_stock_category(stock_category_data, engine)
    stock_supplier_data = join_stock_supplier(stock_supplier_data, engine)

# To make index start from 1
# bill_data.index += 1
# bill_items_data.index += 1

print len(bill_items_data)

stock_category_data.to_csv('category.csv')
stock_product_data.to_csv('product_1.csv')
stock_supplier_data.to_csv('supplier_1.csv')
bill_items_data.to_csv('bill_items_1.csv')
bill_data.to_csv('bill.csv')

# stock_category_data.to_hdf('ptdc_merged_data.h5', key= 'category', mode= 'w')
# stock_supplier_data.to_hdf('ptdc_merged_data.h5', key= 'supplier', mode= 'w')
# stock_product_data.to_hdf('ptdc_merged_data.h5', key= 'product', mode= 'w')
# bill_data.to_hdf('ptdc_merged_data.h5', key= 'bill', mode= 'w')
# bill_items_data.to_hdf('ptdc_merged_data.h5', key= 'bill_items', mode= 'w')

# Writing to a MySQL DB one table at a time and defining their data types

# stock_supplier_data.to_csv('stock_suppliers_list.csv')
# engine = sqlalchemy.create_engine('mysql://root:root@localhost:3306/ptdc_local')
# stock_category_data.to_sql('stock_category', engine, index=False, dtype={'category_id': sqlalchemy.types.SMALLINT(),
#                                                                          'category_code': sqlalchemy.types.VARCHAR(
#                                                                              length=16),
#                                                                          'category_description': sqlalchemy.types.VARCHAR(
#                                                                              length=255),
#                                                                          'parent_category_id': sqlalchemy.types.SMALLINT(),
#                                                                          'is_perishable': sqlalchemy.types.CHAR(
#                                                                              length=1),
#                                                                          'is_modified': sqlalchemy.types.CHAR(
#                                                                              length=1)})
# stock_product_data.to_sql('stock_product', engine, index=False, dtype={'product_id': sqlalchemy.types.INT(),
#                                                                        'product_code': sqlalchemy.types.VARCHAR(
#                                                                            length=16),
#                                                                        'product_bar_code': sqlalchemy.types.VARCHAR(
#                                                                            length=13),
#                                                                        'product_description': sqlalchemy.types.VARCHAR(
#                                                                            length=255),
#                                                                        'is_av_product': sqlalchemy.types.CHAR(
#                                                                            length=1),
#                                                                        'measurement_unit_id': sqlalchemy.types.SMALLINT(),
#                                                                        'category_id': sqlalchemy.types.SMALLINT()})
#
# bill_data.to_sql('bill', engine, index=True,
#                  dtype={'index': sqlalchemy.types.INT(), 'bill_id': sqlalchemy.types.INT(),
#                         'date_created': sqlalchemy.types.DATETIME(),
#                         'bill_uid': sqlalchemy.types.INT(), 'total_amount': sqlalchemy.types.FLOAT(),
#                         'account_number': sqlalchemy.types.VARCHAR(length=6),
#                         'account_name': sqlalchemy.types.VARCHAR(length=50)})
#
# bill_items_data.to_sql('bill_items', engine, index=True, chunksize=5000,
#                        dtype={'index': sqlalchemy.types.INT(), 'bill_item_id': sqlalchemy.types.INT(),
#                               'quantity': sqlalchemy.types.FLOAT(),
#                               'price': sqlalchemy.types.FLOAT(), 'product_id': sqlalchemy.types.INT(),
#                               'bill_id': sqlalchemy.types.INT(),
#                               'product_description': sqlalchemy.types.VARCHAR(length=255),
#                               'bill_uid': sqlalchemy.types.INT(), 'bill_item_uid': sqlalchemy.types.INT()})
# #
# # accounts_data = pd.read_sql('account_pt', engine)
