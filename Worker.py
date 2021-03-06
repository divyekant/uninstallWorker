import os
from google.cloud import bigquery
from datetime import date
import requests
import argparse

# setup CT account data
ap = argparse.ArgumentParser()

ap.add_argument("-a", "--accid", required=True,
   help="Account ID Missing")
ap.add_argument("-p", "--accpc", required=True,
   help="Accoutn Passcode Missing")
ap.add_argument("-gcredpath", "--gcp", required=True,
   help="Google Cred Path Missing")

args = vars(ap.parse_args())

accid = args['accid']

accpc = args['accpc']

gcp = args['gcp']

print accid + " " + accpc

# Setup today's date
today = date.today()

tDate = today.strftime("%Y%m%d")

# Function to call Ct API
def updateCT(objectId,ts):

	url = "https://api.clevertap.com/1/upload"
	payload = "{ \"d\": [ { \"objectId\": \"%s\", \"type\": \"event\", \"evtName\": \"Firebase App Uninstall\", \"ts\":  %s  } ] }" % (objectId,ts)
	headers = {
	    'X-CleverTap-Account-Id': accid,
	    'X-CleverTap-Passcode': accpc,
	    'Content-Type': "application/json",
	    'cache-control': "no-cache",
	    'Postman-Token': "7872ee8d-b229-4f56-a7b3-c1536c55bcb9"
	    }
	response = requests.request("POST", url, data=payload, headers=headers)
	print response.text
	
# transform TS
def changeTS(ts):
	updatedTS = 0
	tts = str(ts)[0:10]
	updatedTS = int(tts)
	return updatedTS

# Setup Creds
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(gcp)

print "Credendtials from environ:" + format(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))

# Construct a BigQuery client object.
client = bigquery.Client()


# Create Query
query = """
SELECT 
cast(event_timestamp as INT64) as event_time_int,
user_prop.value as Object_id
FROM `zeke-160bc.analytics_205516970.events_intraday_%s` AS t, UNNEST(user_properties) AS user_prop
WHERE user_prop.key = "objectId" AND event_name="app_remove" AND TIMESTAMP_DIFF(TIMESTAMP(CURRENT_DATETIME()), TIMESTAMP_MICROS(cast(event_timestamp as INT64)), MINUTE)<5


""" % tDate


query_job = client.query(query)  # Make an API request.


print("The query data:")
for row in query_job:	
	ts = row["event_time_int"]
	ctid =  row["Object_id"]["string_value"]
	updateCT(ctid,changeTS(ts))
			

