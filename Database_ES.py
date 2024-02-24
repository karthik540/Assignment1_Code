import requests, boto3, json

from decimal import Decimal
import datetime;

cuisine_list = ["chinese", "mexican", "italian", "japanese", "korean"]

limit = 20

location = "Manhattan"


current_time = datetime.datetime.now()

def extract_data_yelp(cuisine):
    url = f"https://api.yelp.com/v3/businesses/search?location={location}&term=restaurants&categories={cuisine}&sort_by=best_match&limit={limit}"
    headers = {"accept": "application/json", 'Authorization' : "Bearer " + "vtW3hk_Rr6IRNmVtQtEkxpvfE7nC-osHwJCW5ArdPQpeo3gaidr7jHrxAAx8KsK3axjoXkI-XSxhDHypweUv7M9O-WsHh_iW880bdkxAADV8KzAfFHfp_4JhLPDUZXYx"}
    response = requests.get(url, headers=headers)
    response = json.loads(response.text)
    restaurant_data = []

    for restaurant in response['businesses']:
        name = restaurant['name']

        address = ""
        for add in restaurant['location']['display_address']:
            address += add
        
        coordinates = restaurant['coordinates']

        review_count = restaurant['review_count']

        rating = restaurant['rating']

        zipcode = restaurant['location']['zip_code']

        data = {
            "partition" : restaurant['id'],
            "Name" : name,
            "Address" : address,
            "Coordinates" : coordinates,
            "Review Count" : review_count,
            "Rating" : rating,
            "Zipcode" : zipcode,
            "Rating" : rating,
            "Cuisine" : cuisine,
            "Timestamp" : str(current_time)
        }
        restaurant_data.append(data)

    return restaurant_data

def push_data_dynamo(restaurant_data):
    dynamodb = boto3.resource('dynamodb')
    dbtable = dynamodb.Table('yelp-restaurants')

    for data in restaurant_data:
        data = json.loads(json.dumps(data), parse_float=Decimal)
        #print(data)
        response = dbtable.put_item(
            Item= data
        )
        print(f"### pushed data {data['partition']}")

def create_elastic_json(restaurant_data):
    result = ""

    arr = []

    for restaurant in restaurant_data:
        print(restaurant)
        json1 = {
            "index" : {
                "_index" : "restaurants",
                "_id" : restaurant['partition']
            }
        }

        json1_string = json.dumps(json1)

        result += json1_string

        json2 = {
            "id" : restaurant['partition'],
            "cuisine" : restaurant['Cuisine']
        }

        json2_string = json.dumps(json2)

        arr.append(json1)
        arr.append(json2)

        result += json2_string
    with open("sample.json", 'a') as file:
        for obj in arr:
            file.write(json.dumps(obj) + '\n')

for cuisine in cuisine_list:
    restaurant_data = extract_data_yelp(cuisine)
    #print(restaurant_data)
    create_elastic_json(restaurant_data)

    
    #print(restaurant_data)
    push_data_dynamo(restaurant_data)

    print(f"\nCuisine {cuisine} is Pushed !")

