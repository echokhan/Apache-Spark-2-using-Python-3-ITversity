import os
import functools as ft
import itertools as it
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
#print(order_items[:10])


#1-########################Filter Function#############################



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


#The above functions may have different functionalities, however all involve
#iterating over each record of the colleciton passed in the parameter and filtering
#out records according to a particular condition
#If we are to somehow pass the condition as a parameter, we can create a general-purpose
def myFilter(c, f):
    ordersFiltered = []
    for i in c:
        if(f(i)):
            ordersFiltered.append(i)
    return ordersFiltered


#print("\n\nFiltering COMPLETE data")
ordersFiltered = myFilter(orders, lambda f: f.split(',')[3] == "COMPLETE")
#for i in ordersFiltered[:10]:
#    print(i)

#print("\n\nFiltering data according to date")
ordersFiltered = myFilter(orders, lambda f: (f.split(',')[1])[:10] == "2013-07-25")
#for i in ordersFiltered[:10]:
#    print(i)

#print("\n\nFiltering data according to order_id")
ordersFiltered = myFilter(order_items, lambda f: (f.split(',')[1])[:10] == str(2))
#for i in ordersFiltered[:10]:
#    print(i)


#2-########################Map Function################################


##########################################################
######Get order_id and order_status from orders###########
##########################################################
def get_id_status(orders):
    """Get order_id and order_status from orders(1st and 4th fields of orders data"""
    mappedList = []
    for i in orders:
        mappedList.append((int(i.split(',')[0]), i.split(',')[3]))
    return mappedList

##########################################################
######Get order_id and subtotal from order_items##########
##########################################################
def get_id_subtotal(order_items):
    """Get order_id and subtotal from order_items"""
    mappedList = []
    for i in order_items:
        mappedList.append((int(i.split(',')[1]),float(i.split(',')[4])))
    return mappedList

##########################################################
#######     Get order_month from orders data    ##########
##########################################################
def getmonthdata(orders):
    """Get month field from orders data (Extract month and year from 2nd field)"""
    mappedList = []
    for i in orders:
        mappedList.append((i.split(',')[1])[:7])
    return mappedList


#In the above defined functions, we are trying to map the collections 
#based on specific columns
#What is common between the functions are iteration of the passed columns
#If we are to pass the function to choose columns as a parameter, we might
#make a generic myMap function
def myMap(c, f):
    newC = []
    for i in c:
        newC.append(f(i))
    return newC


#print("\n\nMapping order_id and order_status")
#ordersmapped = myMap(orders, lambda f: (int(f.split(',')[0]), f.split(',')[3]))
#for i in ordersmapped[:10]:
#    print(i)

#print("\n\nMapping order_id and subtotal from order_items")
#ordersmapped = myMap(order_items, lambda f: (int(f.split(',')[1]),float(f.split(',')[4])))
#for i in ordersmapped[:10]:
#    print(i)

#print("\n\nMapping month and year data")
#ordersmapped = myMap(orders, lambda f: (f.split(',')[1])[:7])
#for i in ordersmapped[:10]:
#    print(i)



#3-########################Reduce Function################################
#Usually, reduce functions requires input on which aggregation functions can be applied
#For instance, we need integers or floats that can be added or compared accordingly

orderItemsFiltered = myFilter(order_items, lambda f: int(f.split(',')[1]) == 2)
orderItemsMap = myMap(orderItemsFiltered, lambda f: float(f.split(',')[4]))
print(orderItemsMap)

##########################################################
#############   Summation of collection  #################
##########################################################
def summation(c):
    total = 0
    for i in c:
        total += i
    return total

##########################################################
############   Maximum of collection    ##################
##########################################################
def maximum(c):
    max = c[0]
    for i in c[1:]:
        max = max if(max > i) else i
    return max
##########################################################
#############  Minimum of collection  ####################
##########################################################

def minimum(c):
    min = c[0]
    for i in c[1:]:
        min = min if(min < i) else i
    return min



#In the above defined functions, we are trying to reduced the collections 
#by applying specific operations on them
#What is common between the functions are iteration of the passed collections
#If we are to pass the function to aggregate the input collection, we might
#make a generic myReduce function

def myReduce(c,f):
    Reduced = c[0]
    for i in c[1:]:
        Reduced = f(Reduced, i)
    return Reduced

#print("\n\Sum of subtotal according to order id")
Reduced = myReduce(orderItemsMap, lambda Reduced,f: Reduced + f)
#print(Reduced)

#print("\n\nMaximum of subtotal according to order id")
Reduced = myReduce(orderItemsMap, lambda Reduced,f: Reduced if (Reduced > f) else f)
#print(Reduced)

#print("\n\nMinimum of subtotal according to order id")
Reduced = myReduce(orderItemsMap, lambda Reduced,f: Reduced if (Reduced < f) else f)
#print(Reduced)

########################Using Default Map Reduce APIs################################
orderItemsFiltered = filter(lambda f: int(f.split(',')[1]) == 2, order_items)
orderItemsMap = list(map(lambda f: float(f.split(',')[4]),orderItemsFiltered))
#print(orderItemsMap)
Reduced = ft.reduce(lambda Reduced,f: Reduced + f,orderItemsMap)
#print(Reduced)

########################Using Itertools groupby and map function################################
orderItems = order_items[:10]
orderItems.sort(key = lambda i: int(i.split(',')[1]))
orderItemsGroupByOrderId = it.groupby(orderItems, lambda i: int(i.split(',')[1]))
revenuePerOrder = list(map(lambda t: sum(map(lambda a: float(a.split(',')[4]),t[1])),orderItemsGroupByOrderId))
print(revenuePerOrder)
