from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
from graph_twitter.settings import EC2_PUBLIC_DNS, PASSWORD_DB_GRAPH, USER_DB_GRAPH
import re


class DataBase(object):

    def __init__(self, uri="bolt://" + EC2_PUBLIC_DNS + ":7687", user=USER_DB_GRAPH, password=PASSWORD_DB_GRAPH):
        self.driver = GraphDatabase.driver(uri, auth=basic_auth(user, password), max_connection_lifetime=1800,
                                           max_connection_pool_size=500,
                                           connection_acquisition_timeout=120)

    # returns a session for the transactions
    def get_db(self):
        return self.driver.session()

    # close the conection with the database
    def close(self):
        self.driver.close()

    # returns the ammount of nodes in the graph
    def count_nodes(self):
        db = self.get_db()
        _result = db.run("MATCH (n) RETURN count(n)")
        for record in _result:
            print(record[0])
            for r in record:
                print("key {0}".format(r))

    # creates a node with the label and properties
    def create_node(self, label=None, properties=None, labelAdd=None, propertiesAdd=None):
        db = self.get_db()
        if label == None:
            if labelAdd is not None:
                label = labelAdd
            else:
                raise ValueError("")
        if properties != None:
            _query = "MERGE (n:{0} {1}) \n".format(label, properties)
            if labelAdd is not None:
                if label is labelAdd:
                    _query = "MERGE (n {0}) \n".format(properties)

                if propertiesAdd is not None:
                    _query += "ON CREATE SET n.created = datetime({ timezone: 'America/Bogota' }), " + "n:{0}, n += {1} ".format(
                        labelAdd, propertiesAdd) + \
                              "ON MATCH SET n.modified = datetime({ timezone: 'America/Bogota' }), " + "n:{0}, n += {1} ".format(
                        labelAdd, propertiesAdd) + " RETURN n"
                else:
                    _query += "ON CREATE SET n.created = datetime({ timezone: 'America/Bogota' }), " + "n:{0} ".format(
                        labelAdd) + \
                              "ON MATCH SET n.modified = datetime({ timezone: 'America/Bogota' }), " + "n:{0} ".format(
                        labelAdd) + " RETURN n"
            else:
                _query += "ON CREATE SET n.created = datetime({ timezone: 'America/Bogota' }) \n\
                					ON MATCH SET n.modified = datetime({ timezone: 'America/Bogota' }) \
                					RETURN n"
        else:
            _query = "MERGE (n:{0}) \n".format(label)
            _query += "ON CREATE SET n.created = datetime({ timezone: 'America/Bogota' }) \n\
					ON MATCH SET n.modified = datetime({ timezone: 'America/Bogota' }) \
					RETURN n"
        _result = db.run(_query)

    # for record in _result:
    #	print(record)

    # update a node with the label and properties given
    def update_node(self, label, properties, labelAdd=None, propertiesAdd=None):
        db = self.get_db()
        _query = "MERGE (n:{0} {1}) ".format(label, properties)
        if propertiesAdd is not None:
            if labelAdd is not None:
                _query += "ON CREATE SET n.created = datetime({ timezone: 'America/Bogota' }), " + " n += {1}, n:{0} ".format(
                    labelAdd, propertiesAdd)
                _query += "ON MATCH SET n.modified = datetime({ timezone: 'America/Bogota' }), " + " n += {1}, n:{0} ".format(
                    labelAdd, propertiesAdd)
            else:
                _query += "ON CREATE SET n.created = datetime({ timezone: 'America/Bogota' }), " + " n += {0} ".format(
                    propertiesAdd)
                _query += "ON MATCH SET n.modified = datetime({ timezone: 'America/Bogota' }), " + "n += {0} ".format(
                    propertiesAdd)
            _query += "RETURN n"
        elif labelAdd is not None:
            _query += "ON CREATE SET n.created = datetime({ timezone: 'America/Bogota' }), " + " n:{0} ".format(
                labelAdd)
            _query += "ON MATCH SET n.modified = datetime({ timezone: 'America/Bogota' }), " + " n:{0} ".format(
                labelAdd)
            _query += "RETURN n"

        result = db.run(_query)
        print(result)

    # creates a realation of the direction specified between two nodes with the label and properties
    def create_relationship(self, labelA, propertiesA, labelB, \
                            propertiesB, direction=None, label=None, properties=None):
        if direction != None:
            if direction == 'L':
                if label != None:
                    _query = "MATCH (a:{0} {1}),".format(labelA, propertiesA)
                    _query += "(b:{0} {1}) ".format(labelB, propertiesB)
                    if properties != None:
                        _query += "MERGE (a)<-[r:{0} {1}]-(b)".format(label, properties)
                        _query += "ON CREATE SET r.created = datetime({ timezone: 'America/Bogota' }) \n\
									ON MATCH SET r.modified = datetime({ timezone: 'America/Bogota' }) \
									RETURN r"
                    else:
                        _query += "MERGE (a)<-[r:{0}]-(b)".format(label)
                        _query += "ON CREATE SET r.created = datetime({ timezone: 'America/Bogota' }) \n\
									ON MATCH SET r.modified = datetime({ timezone: 'America/Bogota' }) \
									RETURN r"
                else:
                    _query = "MATCH (a:{0} {1}),".format(labelA, propertiesA)
                    _query += "(b:{0} {1}) ".format(labelB, propertiesB)
                    _query += "MERGE (a)<--(b)"
            elif direction == 'R':
                if label != None:
                    _query = "MATCH (a:{0} {1}),".format(labelA, propertiesA)
                    _query += "(b:{0} {1}) ".format(labelB, propertiesB)
                    if properties != None:
                        _query += "MERGE (a)-[r:{0} {1}]->(b)".format(label, properties)
                        _query += "ON CREATE SET r.created = datetime({ timezone: 'America/Bogota' }) \n\
									ON MATCH SET r.modified = datetime({ timezone: 'America/Bogota' }) \
									RETURN r"
                    else:
                        _query += "MERGE (a)-[r:{0}]->(b)".format(label)
                        _query += "ON CREATE SET r.created = datetime({ timezone: 'America/Bogota' }) \n\
									ON MATCH SET r.modified = datetime({ timezone: 'America/Bogota' }) \
									RETURN r"
                else:
                    _query = "MATCH (a:{0} {1}),".format(labelA, propertiesA)
                    _query += "(b:{0} {1}) ".format(labelB, propertiesB)
                    _query += "MERGE (a)-->(b)"
            else:
                raise ValueError("Only input (L) left and (R) right")
        else:
            if label != None:
                _query = "MATCH (a:{0} {1}),".format(labelA, propertiesA)
                _query += "(b:{0} {1}) ".format(labelB, propertiesB)
                if properties != None:
                    _query += "MERGE (a)-[r:{0} {1}]-(b)".format(label, properties)
                    _query += "ON CREATE SET r.created = datetime({ timezone: 'America/Bogota' }) \n\
								ON MATCH SET r.modified = datetime({ timezone: 'America/Bogota' }) \
								RETURN r"
                else:
                    _query += "MERGE (a)-[r:{0}]-(b)".format(label)
                    _query += "ON CREATE SET r.created = datetime({ timezone: 'America/Bogota' }) \n\
								ON MATCH SET r.modified = datetime({ timezone: 'America/Bogota' }) \
								RETURN r"
            else:
                _query = "MATCH (a:{0} {1}),".format(labelA, propertiesA)
                _query += "(b:{0} {1}) ".format(labelB, propertiesB)
                _query += "MERGE (a)--(b)"

        _db = self.get_db()
        _result = _db.run(_query)

    # for record in _result:
    #	print(record)

    # structures the data for storage into the graph database like attributes of a node
    def structure_data(self, data, id_graph=None):
        if id_graph != None:
            _data = "{" + "id: \"{0}\",".format(id_graph)
        else:
            _data = "{"
        _count = 0
        for key in data:
            if type(data[key]) == str:
                if not re.search(r'(date|time)', key):
                    _temp = "{0}:\"{1}\"".format(key, data[key])
                else:
                    a = str(data[key])
                    _temp = "{0}: datetime(\"{1}\")".format(key, self.convert_datetime(a))
            else:
                if key == "created" or key == "modified":
                    a = str(data[key])
                    _temp = "{0}: datetime(\"{1}[America/Bogota]\")".format(key, a)
                else:
                    a = data[key]
                    _temp = "{0}:{1}".format(key, a)
            _data += _temp
            _count += 1
            if _count < len(data) and _temp != "":
                _data += ","
                _temp = ""

        _data += "}"
        return _data

    # returns a datetime element
    def convert_datetime(self, sdatetime):
        try:
            datetime_object = datetime.strptime(sdatetime, '%Y-%m-%d %H:%M:%S')
            datetime_object = sdatetime[:10] + "T" + sdatetime[10:]
        except Exception as e:
            return sdatetime
        return datetime_object.replace(" ", "")

    def get_campaigns(self, user_id):
        _db = self.get_db()
        _properties = {"user_id": user_id}
        _properties = self.structure_data(_properties)
        _query = "MATCH (c:campaign)<-[:creates]-(n:user" + "{0}".format(_properties) + ") return c"
        _result = _db.run(_query)
        if _result is not None:
            return _result
        else:
            return None

    # returns true if the node with de properties and label specified are into graph
    def is_node(self, label, properties):
        _db = self.get_db()
        _properties = self.structure_data(properties)
        _query = "MATCH (n:{0} {1})".format(label, _properties) \
                 + "RETURN n"
        _run = _db.run(_query)
        _node = None
        for record in _run:
            for node in record:
                _node = node
        return False if _node is None else True

    def is_relation(self, label_a: str, relation_label: str, label_b: str, properties_a: dict = None,
                    properties_b: dict = None):
        db = self.get_db()
        if properties_a is None and properties_a is None:
            query = "MATCH (:{0})-[r:{2}]-(:{1}) RETURN r AS relation".format(label_a, label_b, relation_label)
        elif properties_a is not None:
            props_a = self.structure_data(properties_a)
            query = "MATCH (:{0} {3})-[r:{2}]-(:{1}) RETURN r AS relation".format(label_a, label_b, relation_label,
                                                                                  props_a)
        elif properties_b is not None:
            props_b = self.structure_data(properties_b)
            query = "MATCH (:{0})-[r:{2}]-(:{1} {3}) RETURN r AS relation".format(label_a, label_b, relation_label,
                                                                                  props_b)
        else:
            props_a = self.structure_data(properties_a)
            props_b = self.structure_data(properties_b)
            query = "MATCH (:{0} {3})-[r:{2}]-(:{1} {4}) RETURN r AS relation".format(label_a, label_b, relation_label,
                                                                                      props_a, props_b)
        run = db.run(query)
        _node = None
        for record in run:
            for node in record:
                _node = node
        return False if _node is None else True

    # returns a dic with the labels and properties of the node id given
    def get_node(self, node_id):
        db = self.get_db()
        _query = "MATCH (n) WHERE n.id = \"{0}\" or n.client_id = \"{0}\" RETURN n, labels(n) as labels".format(node_id)
        _run = db.run(_query)
        for record in _run:
            for node in record:
                return self.convert_to_dic(node.items(), record["labels"])

    # returns a dic with the labels and properties
    def get_node_2(self, label, properties, tag=None):
        db = self.get_db()
        _query = "MATCH (n:{0} {1})".format(label, properties)
        _query += "<-[:{0}]-(m) RETURN m, labels(m) as labels".format(tag) if tag is not None else "RETURN null"
        _run = db.run(_query)
        for record in _run:
            print(record.items())
            for node in record:
                return self.convert_to_dic(node.items(), record["labels"])

    # returns a dic with the labels and properties
    def get_node_3(self, label, properties):
        db = self.get_db()
        _query = "MATCH (n:{0} {1})".format(label, properties)
        _query += "RETURN n"
        _run = db.run(_query)
        for record in _run:
            # print(record.items())
            for node in record:
                return self.convert_to_dic(node.items())

    def delele_similar(self):
        db = self.get_db()
        _query = "match ()-[r:SIMILAR]->() delete r"
        _result = db.run(_query)

        for record in _result:
            return record

    def convert_to_dic(self, data, labels=None):
        if labels is not None:
            _dic = {"labels": ":".join(labels)}
            for key, item in data:
                _dic[key] = item
            return _dic
        else:
            _dic = {}
            for key, item in data:
                _dic[key] = item
            return _dic

    def get_campaing_nosql_id(self, campaign_id):
        nosql_id = None
        _db = self.get_db()
        _properties = {"campaign_id": campaign_id}
        _properties = self.structure_data(_properties)
        _query = "MATCH (c:campaign) WHERE c.id = " + "{0}".format(_properties) + " RETURN c.nosql_id"
        _result = _db.run(_query)
        if _result is not None:
            for record in _result:
                nosql_id = record[0]
        return nosql_id

    def get_id_user_campaign_creator(self, campaign_id):
        user_id = None
        _db = self.get_db()
        _properties = {"nosql_id": campaign_id}
        _properties = self.structure_data(_properties)
        _query = "MATCH (c:campaign " + "{0}".format(_properties) + ")<-[:creates]-(u:user) RETURN u.user_id"
        _result = _db.run(_query)
        if _result is not None:
            for record in _result:
                user_id = record[0]
        return user_id

    def get_user_client_accounts(self, user_id):
        db = self.get_db()
        properties = {"user_id": user_id}
        properties = self.structure_data(properties)
        query = "MATCH (n:client_account)<-[:creates]-(u:user" + "{0}".format(properties) + ") RETURN n"
        result = db.run(query)
        if result is not None:
            return self.get_list_of_query(result)
        else:
            return None

    def get_client_account_campaigns(self, client_id):
        db = self.get_db()
        properties = {"client_id": client_id}

        properties = self.structure_data(properties)
        query = "MATCH (n:campaign)<-[:has]-(c:client_account" + "{0}".format(properties) + ") RETURN n"
        result = db.run(query)
        if result is not None:
            return self.get_list_of_query(result)
        else:
            return None

    def get_social_networks_competitor(self, competitor_id):
        db = self.get_db()
        properties = {"id": competitor_id}
        properties = self.structure_data(properties)
        query = "MATCH (n:competitor" + "{0}".format(
            properties) + ")-[:is_linked]->(s:social_network) RETURN s, labels(s)"
        result = db.run(query)
        if result is not None:
            return self.get_list_social_networks(result)
        else:
            return None

    def get_social_network_client_account(self, client_id):
        db = self.get_db()
        properties = {"client_id": client_id}
        properties = self.structure_data(properties)
        query = "MATCH (n:client_account" + "{0}".format(
            properties) + ")-[:is_linked]->(s:social_network) RETURN s, labels(s)"
        result = db.run(query)
        if result is not None:
            return self.get_list_social_networks(result)
        else:
            return None

    def get_data_tweet(self, tweet_id):
        db = self.get_db()
        properties = {"id": tweet_id}
        properties = self.structure_data(properties)
        query = "MATCH (t:tweet" + "{0}".format(properties) + ") RETURN t"
        result = db.run(query)
        if result is not None:
            return self.get_data_query(result)
        else:
            return None

    def get_mentioned_user_twitter_tweet(self, tweet_id):
        db = self.get_db()
        properties = {"id": tweet_id}
        properties = self.structure_data(properties)
        query = "MATCH (t:tweet" + "{0}".format(properties) + ")-[:mentioned]->(u:twitter) RETURN u"
        result = db.run(query)
        if result is not None:
            return self.get_list_of_query(result)
        else:
            return None

    def get_hashtags_tweet(self, tweet_id):
        db = self.get_db()
        properties = {"id": tweet_id}
        properties = self.structure_data(properties)
        query = "MATCH (t:tweet" + "{0}".format(properties) + ")-[:tags]->(h:hashtag) RETURN h"
        result = db.run(query)
        if result is not None:
            return self.get_list_of_query(result)
        else:
            return None

    def get_source_tweet(self, tweet_id):
        db = self.get_db()
        properties = {"id": tweet_id}
        properties = self.structure_data(properties)
        query = "MATCH (t:tweet" + "{0}".format(properties) + ")-[:using]->(s:source) RETURN s"
        result = db.run(query)
        if result is not None:
            return self.get_list_of_query(result)
        else:
            return None

    def get_links_tweet(self, tweet_id):
        db = self.get_db()
        properties = {"id": tweet_id}
        properties = self.structure_data(properties)
        query = "MATCH (t:tweet" + "{0}".format(properties) + ")-[:contains]->(l:link) RETURN l"
        result = db.run(query)
        if result is not None:
            return self.get_list_of_query(result)
        else:
            return None

    def get_trends(self, woeid):
        db = self.get_db()
        properties = {"woeid": woeid}
        properties = self.structure_data(properties)
        query = "MATCH (n:location" + "{0}".format(properties) + ")-[:trends]->(trends) \
        RETURN trends \
        ORDER BY trends.created \
        ASC \
        LIMIT 50"
        result = db.run(query)
        if result is not None:
            return self.get_list_of_query(result)
        else:
            return None

    def get_users_most_tweets(self, id_research):
        print("entro aqui")
        db = self.get_db()
        props_key_research = self.structure_data(dict(id_research=id_research))
        query = "MATCH (tk:twitter_key_research " + "{0}".format(props_key_research) + ")\
        <-[:belongs]-(t:tweet)<-[:tweeted]-(user:twitter) \
        RETURN user.screen_name AS screen_name, count(*) AS tweets \
        ORDER BY tweets \
        DESC \
        limit 10"
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "screen_name", "tweets")
        else:
            return None

    def get_users_most_retweets(self, id_research):
        db = self.get_db()
        props_key_research = self.structure_data(dict(id_research=id_research))
        query = "MATCH (tk:twitter_key_research " + "{0}".format(props_key_research) + ")\
        -[:belongs]-(t:tweet)-[:retweet_of]-(user:twitter) \
        RETURN user.screen_name AS screen_name, count(*) AS retweets \
        ORDER BY retweets \
        DESC \
        limit 10"
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "screen_name", "retweets")
        else:
            return None

    def get_users_most_mentioned(self, id_research):
        db = self.get_db()
        props_key_research = self.structure_data(dict(id_research=id_research))
        query = "MATCH (tk:twitter_key_research " + "{0}".format(props_key_research) + ")\
        -[:belongs]-(t:tweet)-[tw:mentioned]-(user:twitter) \
        RETURN user.screen_name AS screen_name, count (*) AS mentions \
        ORDER BY mentions \
        DESC \
        limit 10"
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "screen_name", "retweets")
        else:
            return None

    def get_users_most_followed(self, id_research):
        db = self.get_db()
        props_key_research = self.structure_data(dict(id_research=id_research))
        query = "MATCH (tk:twitter_key_research " + "{0}".format(props_key_research) + ")\
        <-[:belongs]-(t:tweet)-[:retweet_of]-(user:twitter) \
        WITH collect({screen_name:user.screen_name, followers_count: user.followers_count}) AS rows \
        MATCH (tk2:twitter_key_research " + "{0}".format(props_key_research) + ")\
        <-[:belongs]-(t2:tweet)-[:tweeted]-(user2:twitter) \
        WITH rows + collect({screen_name:user2.screen_name, followers_count: user2.followers_count}) AS allRows \
        UNWIND allRows AS row \
        WITH row.screen_name AS screen_name, row.followers_count AS followers \
        RETURN screen_name, followers, count(*) AS cont \
        ORDER BY followers \
        DESC"
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "screen_name", "followers")
        else:
            return None

    def get_users_most_engaged(self, id_research):
        db = self.get_db()
        props_key_research = self.structure_data(dict(id_research=id_research))
        query = "MATCH (tk:twitter_key_research " + "{0}".format(props_key_research) + ")\
        <-[:belongs]-(t:tweet)-[:tweeted]-(user:twitter) \
        RETURN user.screen_name AS screen_name, ((sum(t.favorite_count) + sum(t.retweet_count))) AS engagement \
        ORDER BY engagement \
        DESC"
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "screen_name", "engagement")
        else:
            return None

    def get_target_connections(self):
        db = self.get_db()
        query = "MATCH (n:rule_target) \
        OPTIONAL MATCH (n)-[:belongs_to]-(x:twitter) \
        WITH n, x \
        RETURN n.id AS id, count(x) AS processes"
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "id", "processes")
        else:
            return None

    def get_tweet_more_retweeted(self, screen_name):
        screen_name = screen_name.lower()
        db = self.get_db()
        props_twitter = self.structure_data(dict(screen_name=screen_name))
        query = "MATCH (t:twitter {0})-[:tweeted]-(tw:tweet) \
        RETURN tw.id AS id, tw.retweet_count AS retweet_count \
        ORDER BY retweet_count \
        DESC \
        LIMIT 10".format(props_twitter)
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "id", "retweet_count")
        else:
            return None

    def deep_know_tweets(self, screen_name, count):
        screen_name = screen_name.lower()
        db = self.get_db()
        props_twitter = self.structure_data(dict(screen_name=screen_name))
        query = "match (t:tweet)-[ret:retweet_of]->(user:twitter " + "{0}".format(props_twitter) + ") \
        with collect({tweet_id: t.id, id: ret.id, type:'retweet'}) as rows \
        match (t2:tweet)<-[:tweeted]-(user2:twitter " + "{0}".format(props_twitter) + ") \
        with rows + collect({tweet_id: t2.id, id: t2.id, type:'tweet'}) as allRows \
        UNWIND allRows as row \
        WITH row.tweet_id as tweet_id, row.id as order_id, row.type as type \
        return tweet_id, order_id, type \
        order by order_id \
        desc \
        limit " + "{0}".format(count) + ""
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "tweet_id", "order_id", "type")
        else:
            return None

    def get_competitors(self, campaign_id):
        campaign_id = str(campaign_id)
        db = self.get_db()
        props = self.structure_data(dict(nosql_id=campaign_id))
        query = "MATCH (c:campaign " + "{0}".format(props) + ")-[:found]->(co:competitor) \
        RETURN co.id AS competitor_id, co.text AS name_competitor"
        result = db.run(query)
        if result is not None:
            return self.get_list_query(result, "competitor_id", "name_competitor")
        else:
            return None

    def get_data_query(self, query):
        object_client_accounts = {}
        for record in query:
            for k, v in record.items():
                for key, value in v.items():
                    if key == 'created' or key == 'modified':
                        object_client_accounts.update({key: value.__str__()})
                    else:
                        object_client_accounts.update({key: value})
        return object_client_accounts

    def get_list_of_query(self, query):
        dict_client_accounts = []
        for record in query:
            for k, v in record.items():
                object_client_accounts = {}
                for key, value in v.items():
                    if key == 'created' or key == 'modified':
                        object_client_accounts.update({key: value.__str__()})
                    else:
                        object_client_accounts.update({key: value})
                dict_client_accounts.append(object_client_accounts)
        return dict_client_accounts

    def get_list_social_networks(self, query):
        for record in query:
            dict_result = dict()
            for k_record, v_record in record.items():
                if k_record == 's':
                    for k_data, v_data in v_record.items():
                        if k_data == 'url':
                            dict_result.update(url=v_data)
                        if k_data == 'screen_name':
                            dict_result.update(screen_name=v_data)
                else:
                    dict_result.update(type=v_record[1])
            yield dict_result

    def get_list_value_of_query(self, query, val: str):
        dict_client_accounts = []
        for record in query:
            for k, v in record.items():
                for key, value in v.items():
                    if key == val:
                        dict_client_accounts.append(value)
        return dict_client_accounts

    def get_list_query(self, query, *args):
        for record in query:
            dict_result = dict()
            for name_col, val_all_row in record.items():
                for args_col in args:
                    if name_col == args_col:
                        dict_result.update({name_col: val_all_row})
            yield dict_result
