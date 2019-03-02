import json
import uuid

import boto3
from rest_framework import status

from graph_twitter.settings import ACCESS_KEY, ACCESS_ID
from database.database import DataBase


class ConnectAWSServices(object):

    def __init__(self):
        self.dynamodb_client = None
        self.lambda_client = None
        self.cloud_watch_event_client = None
        self.comprehend = None

    def  connect_comprehend(self):
        try:
            self.comprehend = boto3.client(service_name='comprehend', region_name='us-east-1',
                                           aws_access_key_id=ACCESS_ID,
                                           aws_secret_access_key=ACCESS_KEY)
        except Exception:
            raise Exception('Failed to establish connection with aws', status.HTTP_500_INTERNAL_SERVER_ERROR)

    def comprehend_detect_sentiment(self, text, language_code):
        if self.comprehend:
            return self.comprehend.detect_sentiment(Text=text, LanguageCode=language_code)
        else:
            raise Exception('Failed to establish connection with aws', status.HTTP_500_INTERNAL_SERVER_ERROR)

    def comprehend_detect_entities(self, text, language_code):
        if self.comprehend:
            return self.comprehend.detect_entities(Text=text, LanguageCode=language_code)
        else:
            raise Exception('Failed to establish connection with aws', status.HTTP_500_INTERNAL_SERVER_ERROR)

    def lambda_resource(self):
        try:
            self.lambda_client = boto3.client('lambda', region_name='us-east-1', aws_access_key_id=ACCESS_ID,
                                              aws_secret_access_key=ACCESS_KEY)
        except Exception:
            raise Exception('Failed to establish connection with aws', status.HTTP_500_INTERNAL_SERVER_ERROR)

    def cloudwatch_events_resource(self):
        try:
            self.cloud_watch_event_client = boto3.client('events', region_name='us-east-1', aws_access_key_id=ACCESS_ID,
                                                         aws_secret_access_key=ACCESS_KEY)
        except Exception:
            raise Exception('Failed to establish connection with aws', status.HTTP_500_INTERNAL_SERVER_ERROR)

    def dynamodb_resources(self):
        try:
            self.dynamodb_client = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id=ACCESS_ID,
                                                  aws_secret_access_key=ACCESS_KEY)
        except Exception:
            raise Exception('Failed to establish connection with aws', status.HTTP_500_INTERNAL_SERVER_ERROR)

    def create_lambda_function(self, fn_name: str, fn_role: str, s3key: str, runtime='python3.6',
                               s3bucket='bookstore-deep-know-toolia', timeout=300, memory_size=256):
        return self.lambda_client.create_function(
            FunctionName=fn_name,
            Runtime=runtime,
            Role=fn_role,
            Handler='{0}.handler'.format(fn_name),
            Code={
                'S3Bucket': s3bucket,
                'S3Key': '{0}.zip'.format(s3key),
            },
            Timeout=timeout,
            MemorySize=memory_size
        )

    def lambda_add_permission(self, fn_name: str, statement_id: str, source_arn: str, action='lambda:InvokeFunction',
                              principal='events.amazonaws.com'):
        return self.lambda_client.add_permission(
            FunctionName=fn_name,
            StatementId="{0}-event".format(statement_id),
            Action=action,
            Principal=principal,
            SourceArn=source_arn,
        )

    def cw_event_put_rule(self, name: str, role_arn: str, schedule_expression: str, state='ENABLED'):
        return self.cloud_watch_event_client.put_rule(
            Name=name,
            RoleArn=role_arn,
            ScheduleExpression=schedule_expression,
            State=state
        )

    def cw_event_put_targets(self, rule: str, target_arn: str, target_id: str, input_data: str):
        return self.cloud_watch_event_client.put_targets(
            Rule=rule,
            Targets=[
                {
                    'Arn': target_arn,
                    'Id': target_id,
                    'Input': input_data
                }
            ]
        )

    def connect_deep_know_toolia(self):
        return self.dynamodb_client.Table('deep-know-toolia') if self.dynamodb_client is not None else None

    def connect_campaigns_toolia(self):
        return self.dynamodb_client.Table('campaigns-toolia') if self.dynamodb_client is not None else None

    def put_item_dynamodb(self, table_dynamo, *args):
        table_dynamo.put_item(
            Item=args
        )


class GraphAWSService(object):

    def __init__(self):
        self.db_neo4j = DataBase()
        self.aws_services = ConnectAWSServices()
        self.role_arn = 'arn:aws:iam::421129146944:role/test-zappa-dev-ZappaLambdaExecutionRole'
        self.fn_lambda_arn = 'arn:aws:lambda:us-east-1:421129146944:function:twitter_scheduled_event'

    def cloud_watch_event_rule_graph(self):
        self.aws_services.lambda_resource()
        self.aws_services.cloudwatch_events_resource()

        # Graph event rules
        rule_id = str(uuid.uuid4())
        rule_name = 'toolia-twitter-' + rule_id
        response = self.aws_services.cw_event_put_rule(
            name=rule_name,
            role_arn=self.role_arn,
            schedule_expression='cron(0 0-23 * * ? *)'
        )
        rule_arn = response.get('RuleArn')
        props_rule = self.db_neo4j.structure_data(dict(id=rule_id))
        self.db_neo4j.create_node('aws_rule', props_rule)

        # Graph rule targets
        for count in range(5):
            target_id = str(uuid.uuid4())
            self.aws_services.cw_event_put_targets(
                rule=rule_name,
                target_arn=self.fn_lambda_arn,
                target_id=target_id,
                input_data=json.dumps(dict(id=target_id))
            )
            props_rule_target = self.db_neo4j.structure_data(dict(id=target_id))
            self.db_neo4j.create_node('rule_target', props_rule_target)
            self.db_neo4j.create_relationship(
                labelA='rule_target',
                propertiesA=props_rule_target,
                labelB='aws_rule',
                propertiesB=props_rule,
                direction='L',
                label='has'
            )

        # Add permission lambda
        fn_name = 'twitter_scheduled_event'
        self.aws_services.lambda_add_permission(
            fn_name=fn_name,
            statement_id=fn_name + rule_id,
            source_arn=rule_arn
        )

    def least_connection(self):
        target_connections = self.db_neo4j.get_target_connections()
        flag = 9999999999
        least = None
        if target_connections:
            for target in target_connections:
                num_processes = target.get('processes')
                if num_processes < flag:
                    flag = num_processes
                    least = target.get('id')
        else:
            return None
        return least

    def establish_connection(self, screen_name):
        dict_props_twitter = dict(screen_name=screen_name)
        if self.db_neo4j.is_node('twitter', dict_props_twitter):
            if not self.db_neo4j.is_relation('twitter', 'belongs_to', 'rule_target', properties_a=dict_props_twitter):
                target_id = self.least_connection()
                if target_id:
                    props_rule_target = self.db_neo4j.structure_data(dict(id=target_id))
                    props_twitter = self.db_neo4j.structure_data(dict_props_twitter)
                    self.db_neo4j.create_relationship(
                        labelA='twitter',
                        propertiesA=props_twitter,
                        labelB='rule_target',
                        propertiesB=props_rule_target,
                        label='belongs_to',
                        direction='R'
                    )
                else:
                    raise Exception('Targets not found.', status.HTTP_404_NOT_FOUND)
            else:
                raise Exception('Twitter is not valid. The user already has a stream request.',
                                status.HTTP_400_BAD_REQUEST)
        else:
            raise Exception('Twitter not found.', status.HTTP_404_NOT_FOUND)
