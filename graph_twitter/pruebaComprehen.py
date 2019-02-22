import boto3
import json

ACCESS_KEY = 'nzHMl6coJAKaLHsiw2rb5vdZbgpL7ELTkQHOX8oX'
ACCESS_ID = 'AKIAJ42URNWFXTMQJ3NQ'

comprehend = boto3.client(service_name='comprehend', region_name='us-east-1', aws_access_key_id=ACCESS_ID,
                          aws_secret_access_key=ACCESS_KEY)

text = """Que eficacia de la policía, atacar de esa manera a una señora que vende flores. Esa es la gloria de Colombia, pero que uno los llamé por un robo o por una atentado, se hacen los pendejos y no van. """

print('Calling DetectSentiment')
print(json.dumps(comprehend.detect_sentiment(Text=text, LanguageCode='es'), sort_keys=True, indent=4))
print('End of DetectSentiment\n')
