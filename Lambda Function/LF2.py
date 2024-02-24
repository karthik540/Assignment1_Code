import json, boto3, logging, random
#import urllib3
from pip._vendor import requests
from botocore.exceptions import ClientError
logger = logging.getLogger()
logger.setLevel("INFO")

def get_elastic_recommendation(cuisine):
    query = "https://search-restaurant-domain-hg7u5lvj4afni6mbgels2beyhm.aos.us-east-1.on.aws/restaurants/_search?q=cuisine:chinese"
    response = requests.get(query, auth=("deku", "Welcome@540"))
    print(response)
    data = json.loads(response.content.decode('utf-8'))
    esdata = data['hits']['hits']
    nums = random.sample(range(0, len(esdata)-1), 5)
    
    restaurant_ids = []
    for i in range(5):
        restaurant_ids.append(esdata[i]['_id'])
    print(restaurant_ids)
    
    return restaurant_ids

def send_email(restaurant_ids, Location, Cuisine, NumberOfPeople, DiningTime, Email):
    
    dynamodb = boto3.resource('dynamodb')
    dbtable = dynamodb.Table('yelp-restaurants')
    sesclient = boto3.client('ses')
    
    BodyContent = f"<h2>Here are your restaurant recommendations</h2> <br> <b>Location</b>: {Location} <br> <b>Cuisine:</b> {Cuisine} <br> <b>Number of People:</b> <b>{NumberOfPeople}</b> <br> <b>Time:</b> {DiningTime} <br>"
    
    
    BodyContent += "<table><tr><th colspan='2'>Restaurant Suggestion List</th></tr><tr><th>Restaurant Name</th><th>Location</th></tr>"
    for id in restaurant_ids:
        response = dbtable.get_item(Key={"partition": id})
        BodyContent += f"<tr><td>{response['Item']['Name']}</td><td>{response['Item']['Address']}</td></tr>"
    
    BodyContent += "</table>"
    
    response = sesclient.send_email(
        Destination={
            "ToAddresses": [
                Email,
            ],
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": "UTF-8",
                    "Data": str(BodyContent),
                }
            },
            "Subject": {
                "Charset": "UTF-8",
                "Data": "Restaurant Recommendation",
            },
        },
        Source="rajkarthik967@gmail.com",
    )
    return response


def lambda_handler(event, context):
    # TODO implement
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName='SuggestionRequestQueue')
    
    logger.info("Got queue '%s' with URL=%s", 'SuggestionRequestQueue', queue.url)
    
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )
        if len(messages) == 0:
            return {
                'statusCode': 200,
                'body': json.dumps('No Messages Found !')
            }
        message = messages[0].body
        receipt_handle = messages[0].receipt_handle
        print(receipt_handle)
        logger.info("Received message: %s: %s", messages[0].message_id, messages[0].body)
        message = message.replace("\'", "\"")
        message = json.loads(message)
        print(type(message))
        print(message)
        
        Location = message['Location']
        Cuisine = message['Cuisine']
        NumberOfPeople = message['NumberOfPeople']
        DiningTime = message['DiningTime']
        Email = message['Email']
        
        restaurant_ids = get_elastic_recommendation(Cuisine)
        
        email_response = send_email(restaurant_ids, Location, Cuisine, NumberOfPeople, DiningTime, Email)
        
        print("comes here")
        print(email_response)
        
        if email_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            delete_response = queue.delete_messages(
                Entries=[
                    {
                        'Id': "1",
                        'ReceiptHandle': receipt_handle
                    },
                ]
            )
            print("message deleted from queue")
            print(delete_response)
        
        
        
        
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    
    return {
        'statusCode': 200,
        'body': json.dumps('Job Ran Successfully !')
    }
