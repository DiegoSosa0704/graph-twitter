#!/usr/bin/python3
import requests
import sys

args = sys.argv
event = {
    "keyword": args[1],
    "lang": "es",
    "count": int(args[2]),
    "geocode": "5.533,-73.367,15km",
    "campaign_id": args[3]
}
requests.post('http://10.4.44.114/api/v1/search_tweet_geocode/', data=event)
