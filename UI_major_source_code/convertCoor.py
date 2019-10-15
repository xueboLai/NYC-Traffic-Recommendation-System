import urllib
import json
import codecs
import re

#my google map credential
#it's taken out due to privacy reason: please contact us to have the key
#key="AIzaSyD-O8o1ZaMQffgEKoe9eBftpxXUbLUJxTY"
KEY = "AIzaSyC0gjbH7Na-CJlK8frrF3SRdWNZUWmJLLc"

#test address
#https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&key=YOUR_API_KEY
address="257+Gold+Street+Brooklyn"

#set the reader to be able to process utf-8 value
reader = codecs.getreader("utf-8")

#find the geo-coordinates by its address
def find_location_geo_by_address(address,key=KEY,postfix = ""):
    #delete all the punctuation in the sentence; substitutes them by +
    address = re.sub("([^\w]|_)+","+",address)
    #add postfix if there is
    address = address+postfix
    reader = codecs.getreader("utf-8")
    #fix part of the url plus the formatted part
    url = "https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}".format(address,key)
    #get the returned json value
    answer = json.load(reader(urllib.request.urlopen(url)))
    print("Address is {}".format(address))
    print(answer["results"][0]["geometry"]["location"])
    #return dictionary of geometry coordinates
    return answer["results"][0]["geometry"]["location"]

