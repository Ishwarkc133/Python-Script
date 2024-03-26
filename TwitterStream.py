from time import time
import tweepy
import pandas as pd
from datetime import datetime
import calendar
import mysql.connector

consumer_key = "x"
consumer_secret="x"
access_token="x-x"
access_token_secret="x"

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="x",
  database ='dims'
)

my_cursor = mydb.cursor()


keywords= [ "tornado","earthquake","tsunami","cyclone","hurricane",
            "flood","rain","rainstorm","storm"
            ]

class Listner(tweepy.Stream):

    tweets = []
    time_initiated = datetime.now()
    future_time = time_initiated + pd.DateOffset(hours=1)
    

    def on_status(self,status):
        print("FETCHING TWEET")
        self.tweets.append(status) 

        if(self.future_time <= datetime.now()):
            #ya chai db ma execute
            print("WRITING IN DATABASE")
            tweet_data = []
            for tweet in self.tweets:
                if not tweet.truncated:
                    tweet_data .append([tweet.created_at,tweet.text])
                else:
                    tweet_data .append([tweet.created_at,tweet.extended_tweet['full_text']])

            categorise_tweet(tweet_data)
            self.future_time = self.future_time + pd.DateOffset(hours=4)
            self.tweets=[]
            print("WRITING COMPLETE")



def insert_in_mysql(value):
    sql= "insert into tweets(Created_At,Tweet,Category) values(%s,%s,%s)"
    value = (value)
    my_cursor.execute(sql,value)
    mydb.commit()




def categorise_tweet(tweets):
    count = 0
    try:
        for key,val in tweets:
            print(f"TOTAL TWEETS FETCHED IS {len(tweets)}" )
            print(f"TOTAL TWEETS Remaining IS {len(tweets)-count}" )
            for disaster in keywords:
                if disaster in val:
                    timestamp = calendar.timegm(key.timetuple())
                    value = [datetime.utcfromtimestamp(timestamp),val,disaster]
                    insert_in_mysql(value)
            count = count + 1
    except:
        print("AN EXCEPTION OCCURED")



print("Starting Stream")

stream_tweets = Listner(consumer_key,consumer_secret,access_token,access_token_secret)
stream_tweets.filter(track=keywords)

print("Fetching Complete")




#inefficient Code below

# from openpyxl import Workbook 

# keywords= [ "disaster","tornado","earthquake","tsunami","cyclone","snowstorm","windstorm","hurricane",'sandstorm',"thunderstorm",
#             "avalanche","fire","flood","forest fire","hail","hailstorm","heat","arson",
#             "iceberg","lava","volcano","eruption","magma","rain","rainstorm","richter scale",
#             "seismic","storm"
#             ]



# for word in keywords:
#     wb = Workbook()
#     ws =  wb.active
#     ws.title = "Sheet1"
#     wb.save(filename = word + '.xlsx')
    


# from time import time
# import tweepy
# import pandas as pd
# from datetime import datetime
# import calendar
# import mysql.connector

# consumer_key = "x"
# consumer_secret="x"
# access_token="x-x"
# access_token_secret="x"

# mydb = mysql.connector.connect(
#   host="localhost",
#   user="root",
#   password="x",
#   database ='dims'
# )

# my_cursor = mydb.cursor()


# keywords= [ "tornado","earthquake","tsunami","cyclone","hurricane",
#             "flood","rain","rainstorm","storm"
#             ]

# class Listner(tweepy.Stream):

#     tweets = []
#     time_initiated = datetime.now()
#     future_time = time_initiated + pd.DateOffset(hours=4)
    

#     def on_status(self,status):
#         self.tweets.append(status) 

#         if(self.future_time <= datetime.now()):
#             print("DISCONNECTING STREAM")
#             self.disconnect()



# def insert_in_mysql(value,disaster_type):
#     print("Writing in database")
#     df = pd.read_excel(disaster_type + ".xlsx",index_col=[0], sheet_name='Sheet1', engine='openpyxl')
#     df = df.append(value, ignore_index = True)
#     df.to_excel(disaster_type + '.xlsx')
#     print("Writing Complete")




# def categorise_tweet(tweets):
#     count = 0
#     print('Categorising Tweet')
#     try:
#         for key,val in tweets:
#             print(f"TOTAL TWEETS FETCHED IS{len(tweets)}" )
#             print(f"TOTAL TWEETS Remaining IS{len(tweets)-count}" )
#             for disaster in keywords:
#                 if disaster in val:
#                     timestamp = calendar.timegm(key.timetuple())
#                     value = {"Timestamp":datetime.utcfromtimestamp(timestamp),"Tweet":val,"Category":disaster}
#                     insert_in_mysql(value, disaster)
#             count = count + 1
#     except:
#         print("AN EXCEPTION OCCURED")


# data = []

# print("Starting Stream")

# stream_tweets = Listner(consumer_key,consumer_secret,access_token,access_token_secret)
# stream_tweets.filter(track=keywords)

# for tweet in stream_tweets.tweets:
#     if not tweet.truncated:
#         data.append([tweet.created_at,tweet.text])
#     else:
#         data.append([tweet.created_at,tweet.extended_tweet['full_text']])


# categorise_tweet(data)
# print("Fetching Complete")



# from openpyxl import Workbook 

# keywords= [ "disaster","tornado","earthquake","tsunami","cyclone","snowstorm","windstorm","hurricane",'sandstorm',"thunderstorm",
#             "avalanche","fire","flood","forest fire","hail","hailstorm","heat","arson",
#             "iceberg","lava","volcano","eruption","magma","rain","rainstorm","richter scale",
#             "seismic","storm"
#             ]



# for word in keywords:
#     wb = Workbook()
#     ws =  wb.active
#     ws.title = "Sheet1"
#     wb.save(filename = word + '.xlsx')
    
