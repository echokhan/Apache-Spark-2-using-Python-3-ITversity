import os
#Function to return a list of string records from a csv file
def readData(dataPath):
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList
print(os.getcwd())
print(os.path.exists("data\\retail_db\\orders\\part-00000"))
ordersPath = "data\\retail_db\\orders\\part-00000"

orders = readData(ordersPath)
print(orders[:10])

order_itemsPath = "data\\retail_db\\order_items\\part-00000"
order_items = readData(order_itemsPath)
print(order_items[:10])



