import pandas as pd
import requests
import json
import boto3
from datetime import date,datetime

def lambda_handler(event, context):

    return None
    


url1 = 'https://www.tomtom.com/traffic-index/_next/data/cLpgTfJvmcwXzPAkCVQr3/malaysia-country-traffic.json?cityOrCountry=malaysia-country-traffic'
url2 = 'https://api.midway.tomtom.com/ranking/liveHourly/MYS_kuala-lumpur'

u1_main = json.loads(requests.get(url1).text)['pageProps']['cityOrCountry']['cities'][0]
df1 = pd.json_normalize(u1_main)

# %%
u2_main = json.loads(requests.get(url2).text)['data']
df2 = pd.json_normalize(u2_main)
df2['UpdateTime'] = pd.to_datetime(df2['UpdateTime'], unit = 'ms')
df2['UpdateTime'] = df2['UpdateTime'] + pd.Timedelta(hours=8)
df2['UpdateTimeWeekAgo'] = pd.to_datetime(df2['UpdateTimeWeekAgo'], unit = 'ms')
df2['UpdateTimeWeekAgo'] = df2['UpdateTimeWeekAgo'] + pd.Timedelta(hours=8)

df2 = df2.rename(columns={'TravelTimeLive': 'live_seconds_per_km', 
            'TravelTimeHistoric': 'usual_seconds_per_km',
            'JamsLength' : 'live_total_km_jams',
            'JamsCount' : 'live_total_jams'
            })



# %%
datetime.now().isoformat()

# %%
df2['scrape_time'] = datetime.now().isoformat()
display(df2.sort_values('JamsDelay', ascending=False))

# %%
file_name = f'kl_{date.today().isoformat()}.csv'
# df2.to_csv(file_name, index=False )

# %%
s3 = boto3.resource('s3',
                    aws_access_key_id='',
                    aws_secret_access_key='')

object = s3.Object('01dev', f'tomtom_kl/{file_name}').upload_file(file_name)


