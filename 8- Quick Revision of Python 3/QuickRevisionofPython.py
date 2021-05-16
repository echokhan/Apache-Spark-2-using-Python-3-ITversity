import pandas as pd

ordersPath = "data\\retail_db\\orders\\part-00000"
order_itemsPath = "data\\retail_db\\order_items\\part-00000"

orders = pd.read_csv(ordersPath, names = ["order_id", "order_date", "customer_id", "order_status"])
order_items = pd.read_csv(order_itemsPath, names = ["item_id", "order_id", "product_id", "quantity", "subtotal", "price"])
#print(orders[:10])
#print(order_items[:10])

#Can access individual and multiple fields of the dataset as follows:
print(order_items[['order_id', 'subtotal']])
print(order_items.groupby(['order_id'])['subtotal'].sum())
#Pandas dataframes can also help perform joins, query and filter the data and perform many other
#powerful operations