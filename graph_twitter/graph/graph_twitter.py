import re
import time
import uuid
from datetime import datetime

import tweepy
from rest_framework import status

from database.database import DataBase
from graph_twitter.settings import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET


class GraphTwitter(object):

    def __init__(self):
        self.auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        self.auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        self.api = None
        self.db_neo4j = None
        self.id_research = None
        self.last_tweet_id = None

    def connect_api_twitter(self):
        if self.api is None:
            try:
                self.api = tweepy.API(self.auth)
            except Exception:
                raise Exception('Tweepy api not found.', status.HTTP_500_INTERNAL_SERVER_ERROR)

    def connect_neo4j_database(self):
        if self.db_neo4j is None:
            try:
                self.db_neo4j = DataBase()
            except Exception:
                raise Exception('Failed to establish connection with neo4j.', status.HTTP_500_INTERNAL_SERVER_ERROR)

    def close_database(self):
        if self.db_neo4j is not None:
            self.db_neo4j.close()
            self.db_neo4j = None

    def user_followers(self, **kwargs):
        ids = self.api.followers_ids(**kwargs)
        props_user2 = self.db_neo4j.structure_data(dict(screen_name=kwargs.get('screen_name')))
        for user_id in ids:
            self.connect_neo4j_database()
            props_user1 = self.get_user_twitter_id(user_id)
            self.relation_user_following(props_user1, props_user2)
            self.close_database()
            print(user_id)

    def get_reweets(self, **kwargs):
        get_retweets = self.api.retweets(**kwargs, tweet_mode='extended')
        try:
            for tweet_obj in get_retweets:
                self.graph_tweet(tweet_obj)
                yield {'screen_name': tweet_obj.user.screen_name.lower(),
                       'id': tweet_obj.user.id}
        except Exception as e:
            print(e)

    def search_tweet(self, **kwargs):
        search_result = self.api.search(**kwargs, tweet_mode='extended')
        try:
            # self.key_research_init(**kwargs)
            print("TOTAL DE RESULTADOS", len(search_result))
            for index, tweet_obj in enumerate(search_result):
                self.graph_tweet(tweet_obj)
                print(index)
        except Exception as e:
            raise e

    def key_research_init(self, **kwargs):
        self.id_research = str(uuid.uuid4())
        props_twitter_key_research = self.db_neo4j.structure_data(
            dict(id_research=self.id_research, key_research=kwargs.get('q')))
        props_campaign = self.db_neo4j.structure_data(dict(nosql_id=self.campaign_id))
        self.db_neo4j.create_node('twitter_key_research', props_twitter_key_research)
        self.relation_campaign_twitter_key_research(props_campaign, props_twitter_key_research)

    def get_trends(self, **kwargs):
        trends = self.api.trends_place(**kwargs)
        try:
            self.graph_trends(trends)
        except Exception as e:
            return e

    def get_available_places_trends(self, country_code: str):
        available_places = self.api.trends_available()
        for place in available_places:
            if place.get('countryCode') == country_code.upper():
                yield place

    def user_tracking(self, **kwargs):
        user_info = self.api.user_timeline(**kwargs, tweet_mode='extended')
        if user_info:
            for index, tweet_obj in enumerate(user_info):
                self.graph_tweet(tweet_obj)
                if index == 0:
                    self.last_tweet_id = tweet_obj.id
            self.add_last_tweet_id(kwargs.get('screen_name'))
        else:
            print("No tiene tweets")

    def graph_trends(self, trends_dict):
        trends_list = trends_dict[0].get('trends')
        region_name = trends_dict[0].get('locations')[0].get('name').lower()
        region_woeid = trends_dict[0].get('locations')[0].get('woeid')
        as_of = self.db_neo4j.convert_datetime(trends_dict[0].get('as_of').__str__())
        created_at = self.db_neo4j.convert_datetime(trends_dict[0].get('created_at').__str__())
        props_region = dict(text=region_name, woeid=region_woeid, as_of=as_of, created_at=created_at)
        props_region = self.get_region(props_region)
        if trends_list:
            for index, trend in enumerate(trends_list):
                name = trend.get('name')
                if self.validate_type_trend(name):
                    props_hashtag = dict(
                        text=name,
                        index=index
                    )
                    props_hashtag = self.create_node_hashtag(props_hashtag)
                    self.relation_region_hashtag(props_region, props_hashtag)
                else:
                    props_sentence = dict(
                        text=name,
                        index=index
                    )
                    props_sentence = self.get_sentence(props_sentence)
                    self.relation_region_sentence(props_region, props_sentence)

    # Devuelve True si es hastag y false si es una frase
    def validate_type_trend(self, trend_name: str):
        if trend_name.find('#') is not -1:
            return True
        else:
            return False

    def graph_tweet(self, tweet_full_obj):
        self.connect_neo4j_database()
        is_retweet = self.is_retweet(tweet_full_obj)
        if is_retweet:
            tweet_obj = tweet_full_obj.retweeted_status
            print(tweet_obj.id)
        else:
            tweet_obj = tweet_full_obj
            print(tweet_obj.id)
        prop_tweet = self.get_tweet(tweet_obj)
        prop_user_tweet = self.get_user_twitter(tweet_obj.user)
        prop_source = self.get_source(tweet_obj)
        self.get_user_twitter_mentioned(tweet_obj, prop_tweet, prop_user_tweet)
        self.get_link(tweet_obj, prop_tweet)
        self.get_hashtag(tweet_obj, prop_tweet)
        if self.id_research is not None:
            props_twitter_key_research = self.db_neo4j.structure_data(
                dict(id_research=self.id_research))
            self.relation_tweet_twitter_key_research(prop_tweet, props_twitter_key_research)
        # if self.screen_name is not None:
        #     props_twitter_profile = self.db_neo4j.structure_data(dict(url='https://twitter.com/' + self.screen_name))
        #     self.relation_twitter_user_timeline(props_twitter_profile, prop_user_tweet)
        if prop_source is not None:
            self.relation_tweet_source(prop_tweet, prop_source)
        if is_retweet:
            # prop_user_tweet -> Creo el tweet
            # prop_user_retweet ->  Retweeteo el tweet (main user)
            prop_user_retweeted = self.get_user_twitter(tweet_full_obj.user)
            props_retweet = self.db_neo4j.structure_data(dict(
                id=tweet_full_obj.id,
                created_at=self.clean_text(tweet_full_obj.created_at.__str__()),
            ))
            self.relation_tweet_user_retweet_of(prop_tweet, prop_user_retweeted, props_retweet)
            self.relation_tweet_user_tweeted(prop_tweet, prop_user_tweet)
            # Relation following
            friendship = self.validate_friendship(tweet_full_obj.user.id, tweet_obj.user.id)
            if friendship.get('following'):
                self.relation_user_following(prop_user_retweeted, prop_user_tweet)
            if friendship.get('followed_by'):
                self.relation_user_following(prop_user_tweet, prop_user_retweeted)
            self.close_database()
            # Reply
            # self.graph_reply(tweet_obj)
        else:
            # prop_user_tweet -> Creo el tweet (main user)
            # self.graph_reply(tweet_obj)
            self.relation_tweet_user_tweeted(prop_tweet, prop_user_tweet)
            self.close_database()

    def get_tweet(self, tweet_obj):
        if tweet_obj is not None:
            properties = dict(
                id=tweet_obj.id,
                text=self.clean_text(tweet_obj.full_text),
                created_at=self.clean_text(tweet_obj.created_at.__str__()),
                retweet_count=tweet_obj.retweet_count,
                favorite_count=tweet_obj.favorite_count,
                lang=tweet_obj.lang
            )
            if not self.db_neo4j.is_node('tweet', dict(id=tweet_obj.id)):
                properties = self.db_neo4j.structure_data(properties)
                self.db_neo4j.create_node('tweet', properties)
                return properties
            else:
                new_props = self.db_neo4j.structure_data(properties)
                self.db_neo4j.update_node('tweet', self.db_neo4j.structure_data(dict(id=tweet_obj.id)),
                                          propertiesAdd=new_props)
                return new_props

    def get_link(self, tweet_obj, prop_tweet):
        urls = tweet_obj.entities.get('urls')
        if urls:
            for url_data in urls:
                properties = dict(
                    url=url_data.get('url'),
                    expanded_url=url_data.get('expanded_url'),
                )
                properties = self.db_neo4j.structure_data(properties)
                self.db_neo4j.create_node('link', properties)
                self.relation_tweet_link(prop_tweet, properties)

    def get_hashtag(self, tweet_obj, prop_tweet):
        hashtags = tweet_obj.entities.get('hashtags')
        if hashtags:
            for url_data in hashtags:
                properties = dict(
                    text=url_data.get('text')
                )
                properties = self.create_node_hashtag(properties)
                # properties = self.db_neo4j.structure_data(properties)
                # self.db_neo4j.create_node('hashtag', properties)
                self.relation_tweet_hashtah(prop_tweet, properties)

    def create_node_hashtag(self, props):
        properties = self.db_neo4j.structure_data(props)
        self.db_neo4j.create_node('hashtag', properties)
        return properties

    def get_user_twitter_mentioned(self, tweet_obj, prop_tweet, prop_user_tweet):
        user_mentions = tweet_obj.entities.get('user_mentions')
        if user_mentions:
            for user_mentioned in user_mentions:
                screen_name = user_mentioned.get('screen_name')
                try:
                    user = self.api.get_user(screen_name=screen_name.lower(), tweet_mode='extended')
                    prop_user_mentioned = self.get_user_twitter(user)
                    self.relation_tweet_user_mentioned(prop_tweet, prop_user_mentioned)
                    # Validate Following
                    if tweet_obj.user.id != user.id and prop_user_tweet != prop_user_mentioned:
                        friendship = self.validate_friendship(tweet_obj.user.id, user.id)
                        if friendship.get('following'):
                            self.relation_user_following(prop_user_tweet, prop_user_mentioned)
                        if friendship.get('followed_by'):
                            self.relation_user_following(prop_user_mentioned, prop_user_tweet)
                except Exception as e:
                    print("Usuairo inexistente", screen_name)

    def get_source(self, tweet_obj):
        if tweet_obj.source:
            properties = dict(
                source_type=tweet_obj.source
            )
            properties = self.db_neo4j.structure_data(properties)
            self.db_neo4j.create_node('source', properties)
            return properties
        else:
            return None

    def is_retweet(self, tweet_obj):
        try:
            if tweet_obj.retweeted_status:
                return True
        except AttributeError:
            return False

    def get_user_twitter_id(self, user_id):
        props = dict(id=user_id)
        if not self.db_neo4j.is_node('twitter', dict(id=user_id)):
            props = self.db_neo4j.structure_data(props)
            self.db_neo4j.create_node('twitter', props)
            return props
        else:
            new_props = self.db_neo4j.structure_data(props)
            return new_props

    def get_user_twitter(self, user_obj):
        if user_obj is not None:
            properties = dict(
                id=user_obj.id,
                name=user_obj.name,
                screen_name=user_obj.screen_name.lower(),
                description=self.clean_text(user_obj.description),
                followers_count=user_obj.followers_count,
                friends_count=user_obj.friends_count,
                statuses_count=user_obj.statuses_count,
                favourites_count=user_obj.favourites_count,
            )
            # properties = self.db_neo4j.structure_data(properties)
            # self.db_neo4j.create_node('twitter', properties)
            if not self.db_neo4j.is_node('twitter', dict(screen_name=user_obj.screen_name.lower())):
                properties = self.db_neo4j.structure_data(properties)
                self.db_neo4j.create_node('twitter', properties)
                return properties
            else:
                # old_props = self.db_neo4j.structure_data(dict(id=user_obj.id))
                new_props = self.db_neo4j.structure_data(properties)
                self.db_neo4j.update_node('twitter',
                                          self.db_neo4j.structure_data(dict(screen_name=user_obj.screen_name.lower())),
                                          propertiesAdd=new_props)
                return new_props
        # return properties

    def graph_reply(self, tweet_obj):
        if tweet_obj.in_reply_to_status_id is not None:
            tweet_reply = self.api.get_status(id=tweet_obj.in_reply_to_status_id, tweet_mode='extended')
            self.graph_tweet(tweet_reply)
            props_tweet = self.db_neo4j.structure_data(dict(id=tweet_obj.id))
            props_reply = self.db_neo4j.structure_data(dict(id=tweet_reply.id))
            self.relation_tweet_reply(props_tweet, props_reply)

    def get_sentence(self, props: dict):
        properties = self.db_neo4j.structure_data(props)
        self.db_neo4j.create_node('sentence', properties)
        return properties

    def get_region(self, props: dict):
        location_text = props.get('text')
        properties = self.db_neo4j.structure_data(props)
        if self.db_neo4j.is_node('location', dict(text=location_text)):
            props_location = self.db_neo4j.structure_data(dict(text=location_text))
            self.db_neo4j.update_node('location', props_location, propertiesAdd=properties)
            return properties
        else:
            self.db_neo4j.create_node('location', properties)
            return properties

    def validate_friendship(self, user_a, user_b):
        info_friendship = self.api.show_friendship(source_id=user_a, target_id=user_b)
        for info in info_friendship:
            if info.id == user_a:
                return dict(
                    following=info.following,  # Si A sigue a B
                    followed_by=info.followed_by  # Si b sigue a A
                )

    def relation_tweet_link(self, prop_tweet, prop_link):
        self.db_neo4j.create_relationship(labelA='tweet', propertiesA=prop_tweet, labelB='link',
                                          propertiesB=prop_link,
                                          direction='R', label='contains')

    def relation_tweet_hashtah(self, prop_tweet, prop_hashtag):
        self.db_neo4j.create_relationship(labelA='tweet', propertiesA=prop_tweet, labelB='hashtag',
                                          propertiesB=prop_hashtag, direction='R', label='tags')

    def relation_tweet_source(self, prop_tweet, prop_source):
        self.db_neo4j.create_relationship(labelA='tweet', propertiesA=prop_tweet, labelB='source',
                                          propertiesB=prop_source, direction='R', label='using')

    def relation_tweet_user_tweeted(self, prop_tweet, prop_user):
        self.db_neo4j.create_relationship(labelA='twitter', propertiesA=prop_user, labelB='tweet',
                                          propertiesB=prop_tweet, direction='R', label='tweeted')

    def relation_tweet_user_mentioned(self, prop_tweet, prop_user):
        self.db_neo4j.create_relationship(labelA='tweet', propertiesA=prop_tweet, labelB='twitter',
                                          propertiesB=prop_user, direction='R', label='mentioned')

    def relation_tweet_user_retweet_of(self, prop_tweet, prop_user, props_retweet):
        self.db_neo4j.create_relationship(labelA='tweet', propertiesA=prop_tweet, labelB='twitter',
                                          propertiesB=prop_user, direction='R', label='retweet_of',
                                          properties=props_retweet)

    def relation_user_following(self, prop_user_a, prop_user_b):
        self.db_neo4j.create_relationship(labelA='twitter', propertiesA=prop_user_a, labelB='twitter',
                                          propertiesB=prop_user_b, direction='R', label='follow')

    def relation_region_sentence(self, props_region, props_sentence):
        self.db_neo4j.create_relationship(labelA='location', propertiesA=props_region, labelB='sentence',
                                          propertiesB=props_sentence, direction='R', label='trends')

    def relation_region_hashtag(self, props_region, prop_hashtag):
        self.db_neo4j.create_relationship(labelA='location', propertiesA=props_region, labelB='hashtag',
                                          propertiesB=prop_hashtag, direction='R', label='trends')

    def relation_campaign_twitter_key_research(self, props_campaign, props_twitter_key_research):
        self.db_neo4j.create_relationship(labelA='campaign', propertiesA=props_campaign,
                                          labelB='twitter_key_research',
                                          propertiesB=props_twitter_key_research, direction='R',
                                          label='was_generated')

    def relation_tweet_twitter_key_research(self, props_tweet, props_twitter_key_research):
        self.db_neo4j.create_relationship(labelA='tweet', propertiesA=props_tweet, labelB='twitter_key_research',
                                          propertiesB=props_twitter_key_research, direction='R', label='belongs')

    def relation_twitter_user_timeline(self, props_twitter, props_user):
        self.db_neo4j.create_relationship(labelA='twitter', propertiesA=props_twitter, labelB='twitter',
                                          propertiesB=props_user, direction='R', label='timeline')

    def relation_tweet_reply(self, props_tweet, props_reply):
        self.db_neo4j.create_relationship(labelA='tweet', propertiesA=props_tweet, labelB='tweet',
                                          propertiesB=props_reply, direction='R', label='reply_of')

    def clean_text(self, text):
        return re.sub(r'[^a-zA-ZñáéíóúÁÉÍÓÚ0-9\s]', r'', text)

    def add_last_tweet_id(self, screen_name):
        self.connect_neo4j_database()
        props_twitter = self.db_neo4j.structure_data(dict(screen_name=screen_name))
        add_props_twitter = self.db_neo4j.structure_data(dict(last_tweet_id=self.last_tweet_id))
        self.db_neo4j.update_node('twitter', props_twitter, propertiesAdd=add_props_twitter)
        self.close_database()

    def set_date(self, date_str):
        date_str = date_str
        time_struct = time.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")  # Tue Apr 26 08:57:55 +0000 2011
        return datetime.fromtimestamp(time.mktime(time_struct))
