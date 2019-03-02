import boto3

ACCESS_KEY = 'nzHMl6coJAKaLHsiw2rb5vdZbgpL7ELTkQHOX8oX'
ACCESS_ID = 'AKIAJ42URNWFXTMQJ3NQ'

comprehend = boto3.client(service_name='comprehend', region_name='us-east-1', aws_access_key_id=ACCESS_ID,
                          aws_secret_access_key=ACCESS_KEY)

text = """Que eficacia de la policía, atacar de esa manera a una señora que vende flores. Esa es la gloria de Colombia, 
pero que uno los llamé por un robo o por una atentado, se hacen los pendejos y no van. """

result = comprehend.detect_sentiment(Text=text, LanguageCode='es')
resultEntiti = comprehend.detect_entities(Text=text, LanguageCode='es')
print(result)
print(resultEntiti.get('Entities'))
