import json
import uuid

from database.database import DataBase
from utils.utils_aws import ConnectAWSServices


class TwitterAnalysis(object):

    def __init__(self, screen_name, user_id, count):
        self.count = count
        self.screen_name = screen_name
        self.user_id = user_id
        self.db_neo4j = DataBase()
        self.aws_services = ConnectAWSServices()
        self.aws_services.lambda_resource()

    def create_analysis(self):
        analysis_id = str(uuid.uuid4())
        props_twitter_analysis = dict(id=analysis_id)
        props_twitter_analysis = self.db_neo4j.structure_data(props_twitter_analysis)
        self.db_neo4j.create_node('twitter_analysis', props_twitter_analysis)

        props_user = dict(user_id=self.user_id)
        props_user = self.db_neo4j.structure_data(props_user)
        self.db_neo4j.create_relationship('user', props_user, 'twitter_analysis', props_twitter_analysis,
                                          direction='R', label='did')

        props_twitter = dict(screen_name=self.screen_name)
        props_twitter = self.db_neo4j.structure_data(props_twitter)
        self.db_neo4j.create_relationship('twitter', props_twitter, 'twitter_analysis', props_twitter_analysis,
                                          direction='R', label='has')
        return analysis_id

    def start_twitter_deep_know(self):
        analysis_id = self.create_analysis()
        data_analysis = self.db_neo4j.deep_know_tweets(self.screen_name, self.count)
        for tweet in data_analysis:
            pay_load = dict(
                type_function="deep_know_twitter",
                type="tweet",
                tweet_id=tweet.get('tweet_id'),
                id_analysis=analysis_id,
            )
            print(pay_load)
            response = self.aws_services.lambda_client.invoke(
                FunctionName='arn:aws:lambda:us-east-1:421129146944:function:lambda_function_deep_know',
                InvocationType='Event',
                Payload=json.dumps(pay_load)
            )
            print(response)