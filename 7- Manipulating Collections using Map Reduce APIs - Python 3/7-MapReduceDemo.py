import os
#Function to return a list of string records from a csv file
def readData(dataPath):
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList


ordersPath = "data\\retail_db\\orders\\part-00000"
orders = readData(ordersPath)
#print(orders[:10])

order_itemsPath = "data\\retail_db\\order_items\\part-00000"
order_items = readData(order_itemsPath)
print(order_items[:10])

##########################################################
######Get COMPLETE orders from orders data set############
##########################################################
def getcompleteorders(orders):
    CompleteOrders = []
    for i in orders:
        if(i.split(',')[3] == 'COMPLETE'):
            CompleteOrders.append(i)
    return CompleteOrders

##########################################################
######Get orders placed on 2013-07-25#####################
########################################################## 
def getdateorders(orders):
    specificdatelist = []
    for i in orders:
        if((i.split(',')[1])[:10] == '2013-07-25'):
            specificdatelist.append(i)
    return specificdatelist

##########################################################
######Get order items for a given order id################
########################################################## 
def itemforid(order_items, order_id):
    order_items_specific_id = []
    for i in order_items:
        if(i.split(',')[1] == str(order_id)):
            order_items_specific_id.append(i)
    return order_items_specific_id

