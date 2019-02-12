from django.urls import re_path, path

from . import views

urlpatterns = [
    re_path(r'^get_data_tweet/(?P<tweet_id>[-\w]+)', views.get_data_tweet, name='get_data_tweet'),
    path('search_tweet_keyword/', views.search_tweet_keyword, name='search_tweet_keyword'),
    path('init_trends_search/', views.trends_search_init, name='init_trends_search'),
    path('user_timeline_search_init/', views.user_timeline_search_init, name='user_timeline_search_init'),
    re_path(r'^get_trends_search/(?P<woeid>[-\w]+)', views.get_trends_search, name='get_trends_search'),
    re_path(r'^twitter/trends/available_places/(?P<country_code>[-\w]+)', views.get_available_places_trends,
            name='get_available_places_trends'),
    path('twitter/tweet/retweets/', views.retweets, name='retweets'),
    re_path(r'^twitter/user/most_tweets/(?P<id_research>[-\w]+)', views.users_most_tweets, name='users_most_tweets'),
    re_path(r'^twitter/user/most_retweets/(?P<id_research>[-\w]+)', views.users_most_retweets,
            name='users_most_tweets'),
    re_path(r'^twitter/user/most_followed/(?P<id_research>[-\w]+)', views.users_most_followed,
            name='users_most_followed'),
    re_path(r'^twitter/user/most_engaged/(?P<id_research>[-\w]+)', views.user_most_engaged,
            name='user_most_engaged'),
    path('twitter/user/start_streaming/<str:screen_name>', views.user_twitter_streaming, name='user_twitter_streaming'),
    path('aws/create_rule', views.create_rule, name='create_rule'),
    path('twitter/user/tweet/most_reweeted/<str:screen_name>', views.get_tweet_more_retweeted,
         name='get_tweet_more_retweeted'),
]