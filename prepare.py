import os

import pandas as pd

from env import get_connection 

def tsa_item_demand():
    filename = 'tsa_item_demand.csv'
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        url = get_connection()
        query = '''
                SELECT 
                    sale_date,
                    sale_amount,
                    item_brand,
                    item_name,
                    item_price,
                    store_address,
                    store_zipcode,
                    store_city,
                    store_state
                FROM
                    items
                        LEFT JOIN
                    sales ON sales.item_id = items.item_id
                        LEFT JOIN
                    stores ON stores.store_id = sales.store_id
                '''
        df = pd.read_sql(query, url)
        df.to_csv(filename, index = False)
        return df


def datetime_index(df):
    df.sale_date = pd.to_datetime(df.sale_date)
    df = df.set_index('sale_date').sort_values(by = 'sale_date')
    return df



def create_column(df):
    df['sale_total'] = df.sale_amount * df.item_price
    return df