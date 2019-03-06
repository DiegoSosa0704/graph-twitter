import requests

event = {
    "keyword": "bicentenario",
    "lang": "es",
    "count": 100,
    "geocode": "5.533,-73.367,15km",
    "campaign_id": "d71824f1-63d0-4ce6-9e7e-020ed5b09de8"
}
requests.post('http://10.4.44.114/api/v1/search_tweet_geocode/', data=event)
