from numpy import place
import requests
import bs4
import datetime
from geotext import GeoText
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="MyApp")
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="x",
  database="dims"
)

mycursor = mydb.cursor()


sql = "INSERT INTO location_info (category, place,lat,lng,date,news) VALUES (%s, %s,%s, %s,%s, %s)"


date = '2022-03-18 10:30:00'
month = date.split(' ')[0].split('-')[1]
month = datetime.date(1900, int(month), 1).strftime('%B')
day = date.split(' ')[0].split('-')[2]

url = 'https://www.google.com/search?q='+'earthquake'+'+in+'+month+'+'+day
request_result=requests.get( url ) 

soup = bs4.BeautifulSoup(request_result.text,"html.parser")

heading_object=soup.find_all( 'h3' )
  
location_data = []

for info in heading_object:
    try:
        place_text = info.getText().replace(month,"")
        places = GeoText(place_text)
        if len(places.cities)>0:
            location = geolocator.geocode(places.cities)
            location_data.append({
                'date':month + " "+ day,
                "place":places.cities[0],
                "lng":location.longitude,
                "lat":location.latitude,
                "news":month + " "+ day +"-"+ info.getText()
            })
        elif len(places.countries)>0:
            location = geolocator.geocode(places.countries)
            location_data.append({
                'date':month + " "+ day,
                "place":places.countries[0],
                "lng":location.longitude,
                "lat":location.latitude,
                "news":info.getText()
            })
    except:
        print("Exception Occured")


keywords= []

for items in location_data:
    if items['place'] not in keywords:
        keywords.append(items['place'])
        val=("rain",items['place'],items['lat'],items['lng'],items['news'],items['date'])
        mycursor.execute(sql,val)
        mydb.commit()





