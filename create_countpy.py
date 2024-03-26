import pandas as pd
from datetime import  datetime
from datetime import timedelta
import statistics


df = pd.read_excel('tornado.xlsx',sheet_name='Sheet1', engine='openpyxl' )
df['Timestamp'] = pd.to_datetime(df['Timestamp'])
start_time = df['Timestamp'][0]

count = []
date_averages = []

loop_end_time = df["Timestamp"][df.shape[0] - 1]
loop_reference = start_time
referenced_date  =  datetime.strptime("2022-02-12 12:41:58",'%Y-%m-%d %H:%M:%S')
date_format_str = '%Y-%m-%d %H:%M:%S'


def add_10_minute(time_str):
    given_time = datetime.strptime(time_str, date_format_str)
    n = 10
    final_time = given_time + timedelta(minutes=n)
    final_time_str = final_time.strftime('%Y-%m-%d %H:%M:%S')
    time_str = final_time_str
    return time_str


while (loop_reference <= loop_end_time):
    end_time  = add_10_minute(str(start_time))
    mask = (df['Timestamp'] > start_time) & (df['Timestamp'] <= end_time)
    sf = df.loc[mask]
    print(f"Eartrhquake Count between-{start_time} and {end_time} is {sf.shape}" )
    start_time = end_time
    loop_reference = datetime.strptime(end_time,date_format_str)
    if sf.shape[0]!=0:
        count.append(sf.shape[0])
    #append in datetime average
    # if end_time ko date part is not equal to the referencend 
    # date part ya chai append gar ani pachi referece model lai chai update gar
    if(referenced_date != end_time[0]):
        date_averages.append(statistics.median(count))
        count = []
        referenced_date = end_time[0]

print(statistics.median(count))

    





