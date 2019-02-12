from operator import itemgetter

from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response

from database.database import DataBase
from graph.graph_twitter import GraphTwitter
from utils.utils_aws import GraphAWSService


@api_view(['GET'])
def get_data_tweet(request, tweet_id):
    print("Token", tweet_id)
    tweet_id = int(tweet_id)
    try:
        database = DataBase()
    except Exception:
        return Response({"detail": "Failed to establish connection with neo4j"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    tweet = database.get_data_tweet(tweet_id)
    mentions = database.get_mentioned_user_twitter_tweet(tweet_id)
    tweet.update({'user_mentions': mentions}) if mentions else tweet.update({'user_mentions': []})
    hashtags = database.get_hashtags_tweet(tweet_id)
    tweet.update({'hashtags': hashtags}) if hashtags else tweet.update({'hashtags': []})
    links = database.get_links_tweet(tweet_id)
    tweet.update({'links': links}) if links else tweet.update({'links': []})
    source = database.get_source_tweet(tweet_id)
    tweet.update({'source': source}) if source else tweet.update({'source': []})

    if tweet:
        return Response(tweet, status.HTTP_200_OK)
    else:
        return Response({"detail": "Node 'tweet' not found"}, status=status.HTTP_404_NOT_FOUND)


@api_view(['POST'])
def search_tweet_keyword(request):
    try:
        keyword = request.data['keyword']
        lang = request.data['lang']
        count = request.data['count']
        campaign_id = request.data['campaign_id']
    except KeyError as e:
        return Response({"detail": "Requires key " + e.__str__()}, status=status.HTTP_400_BAD_REQUEST)
    try:
        map_twitter = GraphTwitter()
        map_twitter.connect_api_twitter()
        map_twitter.connect_neo4j_database()

    except Exception as e:
        return Response({"detail": e.args}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    map_twitter.campaign_id = campaign_id
    map_twitter.search_tweet(q=keyword, lang=lang, count=count)
    return Response({'detail': 'keyword search completed'}, status.HTTP_200_OK)


@api_view(['POST'])
def search_tweet_geocode(request):
    try:
        keyword = request.data['keyword']
        lang = request.data['lang']
        count = request.data['count']
        geocode = request.data['geocode']
    except KeyError as e:
        return Response({"detail": "Requires key " + e.__str__()}, status=status.HTTP_400_BAD_REQUEST)
    try:
        map_twitter = GraphTwitter()
        map_twitter.connect_api_twitter()
        map_twitter.connect_neo4j_database()

    except Exception as e:
        return Response({"detail": e.args}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    map_twitter.campaign_id = campaign_id
    map_twitter.search_tweet(q=keyword, lang=lang, count=count, geocode=geocode)
    return Response({'detail': 'keyword search completed'}, status.HTTP_200_OK)


@api_view(['POST'])
def trends_search_init(request):
    try:
        woeid = request.data['woeid']
    except KeyError as e:
        return Response({"detail": "Requires key " + e.__str__()}, status=status.HTTP_400_BAD_REQUEST)
    try:
        map_twitter = GraphTwitter()
        map_twitter.connect_api_twitter()
        map_twitter.connect_neo4j_database()
        map_twitter.get_trends(id=woeid)
    except Exception as e:
        return Response({"detail": e.args}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return Response({'detail': 'trend search completed'}, status.HTTP_200_OK)


@api_view(['POST'])
def user_timeline_search_init(request):
    try:
        screen_name = request.data['screen_name']
        count = request.data['count']
    except KeyError as e:
        return Response({"detail": "Requires key " + e.__str__()}, status=status.HTTP_400_BAD_REQUEST)
    try:
        map_twitter = GraphTwitter()
        map_twitter.connect_api_twitter()
        map_twitter.connect_neo4j_database()
        map_twitter.user_tracking(screen_name=screen_name, count=count)
    except Exception as e:
        return Response({"detail": e.args}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return Response({'detail': 'user timeline search completed'}, status.HTTP_200_OK)


@api_view(['GET'])
def get_trends_search(request, woeid):
    try:
        database = DataBase()
    except Exception:
        return Response({"detail": "Failed to establish connection with neo4j"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    trends = database.get_trends(int(woeid))
    temp_list = sorted(trends, key=itemgetter('index'))
    if temp_list:
        return Response(temp_list, status.HTTP_200_OK)
    else:
        return Response({"detail": "Trends not found"}, status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
def get_available_places_trends(request, country_code):
    try:
        map_twitter = GraphTwitter()
        map_twitter.connect_api_twitter()
    except Exception as e:
        return Response({"detail": e.args}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    available_places = map_twitter.get_available_places_trends(country_code)
    return Response(list(available_places), status.HTTP_200_OK)


@api_view(['POST'])
def retweets(request):
    try:
        tweet_id = request.data['tweet_id']
        count = request.data['count']
    except KeyError as e:
        return Response({"detail": "Requires key " + e.__str__()}, status=status.HTTP_400_BAD_REQUEST)
    try:
        map_twitter = GraphTwitter()
        map_twitter.connect_api_twitter()
        map_twitter.connect_neo4j_database()
    except Exception as e:
        return Response({"detail": e.args}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    try:
        tweets = map_twitter.get_reweets(id=tweet_id, count=count)
    except Exception as e:
        return Response({'detail': e.args}, status.HTTP_400_BAD_REQUEST)

    return Response(tweets, status.HTTP_200_OK)


@api_view(['GET'])
def users_most_tweets(request, id_research):
    print("entro")
    try:
        database = DataBase()
    except Exception:
        return Response({"detail": "Failed to establish connection with neo4j"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    result_query = database.get_users_most_tweets(str(id_research))
    return Response(result_query, status.HTTP_200_OK)


@api_view(['GET'])
def users_most_retweets(request, id_research):
    try:
        database = DataBase()
    except Exception:
        return Response({"detail": "Failed to establish connection with neo4j"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    result_query = database.get_users_most_retweets(str(id_research))
    return Response(result_query, status.HTTP_200_OK)


@api_view(['GET'])
def users_most_followed(request, id_research):
    try:
        database = DataBase()
    except Exception:
        return Response({"detail": "Failed to establish connection with neo4j"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    result_query = database.get_users_most_followed(str(id_research))
    return Response(result_query, status.HTTP_200_OK)


@api_view(['GET'])
def user_most_engaged(request, id_research):
    try:
        database = DataBase()
    except Exception:
        return Response({"detail": "Failed to establish connection with neo4j"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    result_query = database.get_users_most_engaged(str(id_research))
    return Response(result_query, status.HTTP_200_OK)


@api_view(['GET'])
def user_twitter_streaming(request, screen_name):
    screen_name = screen_name.lower()
    try:
        aws = GraphAWSService()
        aws.establish_connection(screen_name)
        map_twitter = GraphTwitter()
        map_twitter.connect_api_twitter()
        map_twitter.connect_neo4j_database()
    except Exception as e:
        return Response(dict(detail=e.args[0]), e.args[1])
    map_twitter.user_tracking(screen_name=screen_name)
    return Response({'detail': 'Twitter streams started'}, status.HTTP_200_OK)


@api_view((['GET']))
def create_rule(request):
    try:
        aws = GraphAWSService()
    except Exception as e:
        return Response(dict(detail=e.args[0]), e.args[1])
    aws.cloud_watch_event_rule_graph()
    return Response({'detail': 'New rule created'}, status.HTTP_200_OK)


@api_view(['GET'])
def get_tweet_more_retweeted(request, screen_name):
    try:
        database = DataBase()
    except Exception:
        return Response({"detail": "Failed to establish connection with neo4j"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    result_query = database.get_tweet_more_retweeted(screen_name)
    return Response(result_query, status.HTTP_200_OK)


