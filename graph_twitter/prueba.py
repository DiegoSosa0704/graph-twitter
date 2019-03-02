"""
import tweepy

CONSUMER_KEY = 'sjvZbJFS3Mxo1BUHTxENX8nos'
CONSUMER_SECRET = 'BabWvGJ7AjHykhZyh4MPgufk5XmDECVAaItYV6e8SyCYTfgPiy'
ACCESS_TOKEN = '1049783284488654848-9erQLXPtEyCkH8m6dgzdVTrgOiHcHF'
ACCESS_TOKEN_SECRET = 'd5zv2M7dvYeqT3SxSC3i7MEqMPAWlyjEXY5YfigAAVJie'


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

search_result = api.search(q='Venezuela', count=2, tweet_mode='extended', geocode='5.533,-73.367,10km')
print(search_result)
"""

import requests

payload = {
    "keyword": "Venezuela",
    "lang": "es",
    "count": 1,
    "geocode": "5.533,-73.367,15km"
}

r = requests.post('http://127.0.0.1:8000/api/v1/search_tweet_geocode/', data=payload)
print(r.text)
print(r.status_code)
