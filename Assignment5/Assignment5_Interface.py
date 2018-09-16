from pymongo import MongoClient
from math import radians,sin,cos,atan2,sqrt,pow
import os
import sys
import json

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    f1 = open(saveLocation1, "wb")
    for curr_doc in collection.find({'city': { "$regex" : cityToSearch, "$options" : "-i" }}):
        result = str(curr_doc['name']).upper() + "$" + str(curr_doc['full_address']).replace("\n",",").upper() + "$" + str(curr_doc['city']).upper() + "$" + str(curr_doc['state']).upper() + "\n"
        f1.write(result.encode("utf-8"))
    f1.close()
    #pass

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    f2 = open(saveLocation2, "wb")
    myLat = myLocation[0]
    myLon = myLocation[1]

    for curr_doc in collection.find(projection = ['name', 'categories', 'latitude', 'longitude']):
	lat2 = curr_doc['latitude']
	long2 = curr_doc['longitude']
        if(float(getDistance(lat2, long2, float(myLat), float(myLon))) <= float(maxDistance)):
            for category in categoriesToSearch:
                if category in curr_doc['categories']:
                    f2.write(curr_doc['name'].upper().encode("utf-8") + "\n")
                    break
    f2.close()
    #pass

def getDistance(lat2, long2, lat1, long1):
    lat1ang = radians(lat1)
    lat2ang = radians(lat2)
    latDel = radians(lat2 - lat1)
    longDel = radians(long2 - long1)
    a = pow(sin(latDel/2),2) + (cos(lat1ang) * cos(lat2ang) * pow(sin(longDel/2),2))
    c = atan2(sqrt(a),sqrt(1-a)) * 2
    d = 3959 * c
    return d
