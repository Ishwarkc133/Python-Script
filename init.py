keywords= [ "tornado","earthquake","tsunami","cyclone","hurricane",
            "flood","rain","rainstorm","storm"
            ]

#every 20 minutes
#for each keyword every time start run garne
#Classify garera Db ma hali halne

#count difference patta laune  ani db ma halne for each keyword

#Classified ko pani count difference patta laune  ani db ma halne for each keyword


#ani location and news patta laune each time db overwrite hanne 

import pandas as pd
import mysql.connector
from geotext import GeoText
from geopy.geocoders import Nominatim
import requests
import bs4
geolocator = Nominatim(user_agent="MyApp")

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="x",
  database="script"
)


mycursor = mydb.cursor()

column = ['created_at','tweet','category']
date_format_str = '%Y-%m-%d %H:%M:%S'

#dumy tweets
Tweet_List = [
    ["2022-03-18 07:00:03","bdhsbdhsa","rain"],
    ["2022-03-18 07:10:03","bdhsbdhsa","rain"],
    ["2022-03-18 07:11:03","bdhsbdhsa","earthquake"],
    ["2022-03-18 07:12:03","bdhsbdhsa","earthquake"],
    ["2022-03-18 07:13:03","bdhsbdhsa","earthquake"],
    ["2022-03-18 07:23:03","bdhsbdhsa","earthquake"],
    ["2022-03-18 07:24:03","bdhsbdhsa","rain"],
    ["2022-03-18 07:25:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 07:27:03","bdhsbdhsa","rain"],
    ["2022-03-18 07:30:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 07:33:03","bdhsbdhsa","earthquake"],
    ["2022-03-18 07:34:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 07:56:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 08:14:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 08:15:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 08:16:03","bdhsbdhsa","earthquake"],
    ["2022-03-18 08:17:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 08:18:03","bdhsbdhsa","rain"],
    ["2022-03-18 08:19:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 08:20:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 08:21:03","bdhsbdhsa","rain"],
    ["2022-03-18 08:22:03","bdhsbdhsa","earthquake"],
    ["2022-03-18 08:23:03","bdhsbdhsa","cyclone"],
    ["2022-03-18 08:23:10","bdhsbdhsa","rain"],
    ["2022-03-18 08:34:03","bdhsbdhsa","rain"],
    ["2022-03-18 08:35:03","bdhsbdhsa","cyclone"],
]


import schedule
import time
from datetime import  datetime
from datetime import timedelta
import datetime as dt
import statistics

def generelised_tweet(processedData):

        ref_date = "2022-03-22 18:30:15"
        on_time = []
        diaster_period = []
        count =[]

        for dicx in processedData:
            count.append(dicx['count'])

        median = statistics.median(count)

        for timestamps in processedData:

            if timestamps['count'] >= median*5:
                on_time.append(timestamps['created_at'])

        ref_date = "2022-03-22 18:30:15"

        for timestamp in on_time:
            if add_10_minute(ref_date)!=timestamp:
                diaster_period.append(timestamp)
            ref_date = timestamp
        return diaster_period

        




def get_locations(key, timestamps):
    sql_syntax = f'insert into {key} (created_at,count) values (%s,%s)'
    for t in timestamps:
        date = '2022-03-18 10:30:00'
        month = t.split(' ')[0].split('-')[1]
        datetime_object = dt.datetime.strptime(month, "%m")
        month = datetime_object.strftime("%b")
        day = date.split(' ')[0].split('-')[2]
        url = 'https://www.google.com/search?q='+key+'+in+'+month+'+'+day
        request_result=requests.get( url )
        soup = bs4.BeautifulSoup(request_result.text,
                                "html.parser")
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
                        "news":info.getText()
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


        obtained= []

        for items in location_data:
            if items['place'] not in obtained:
                obtained.append(items['place'])
                val=("rain",items['place'],items['lat'],items['lng'],items['news'],items['date'])
                mycursor.execute(sql_syntax,val)
                mydb.commit()


    





def add_10_minute(time_str):
        given_time = datetime.strptime(time_str, date_format_str)
        n = 10
        final_time = given_time + timedelta(minutes=n)
        final_time_str = final_time.strftime('%Y-%m-%d %H:%M:%S')
        time_str = final_time_str
        return time_str


def process_tweet(key):
    sql_syntax = f'insert into {key} (created_at,count) values (%s,%s)'
    data_dict = {}
    data_list = []
    df['created_at'] = pd.to_datetime(df['created_at']).apply(lambda t: t.replace(tzinfo=None))
    start_time = df['created_at'][0]
    loop_end_time = df["created_at"][df.shape[0] - 1]
    loop_reference = start_time
        
    while (loop_reference <= loop_end_time):
        end_time  = add_10_minute(str(start_time))
        mask = (df['created_at'] > start_time) & (df['created_at'] <= end_time)
        sf = df.loc[mask]
        data_dict['created_at']=start_time
        data_dict['count']= sf.shape[0]
        data_list.append(data_dict)
        data_dict= {}
        start_time = end_time
        loop_reference = datetime.strptime(end_time,date_format_str)

    for data in data_list:
        val=(data['created_at'],data['count'])
        mycursor.execute(sql_syntax,val)
        mydb.commit()

    return data_list


    
#evry keyword ko lagi run huncha
def keyword_selector():
    for key in keywords:
        start(key)



def start(key):
    #classify()
    #every keyword process ma jancha
    processed = process_tweet(key)
    timestamps = generelised_tweet(processedData=processed)
    get_locations(key,timestamps)



df = pd.DataFrame(data=Tweet_List,columns=column)
schedule.every(5).seconds.do(keyword_selector)


while True:
    schedule.run_pending()
    time.sleep(1)


