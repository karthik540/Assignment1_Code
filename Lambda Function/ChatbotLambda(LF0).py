import json, boto3

def lambda_handler(event, context):
    # TODO implement
    print("Comes here")
    print(event['messages'][0]['type'])
    print(event['messages'][0]['unstructured']['text'])
    
    
    # Bot id = A5HJTHX7S0
    # alias id = YFLBJPZIT4
    # local id = en_US
    
    client = boto3.client('lexv2-runtime')

    # Submit the text 'I would like to see a dentist'
    
    response = client.recognize_text(
        botId='A5HJTHX7S0',
        botAliasId='TSTALIASID',
        localeId='en_US',
        sessionId="test_session",
        text=event['messages'][0]['unstructured']['text'])
    
    print(response['messages'][0]['content'])
        
    print(response)
    #logger.info(str(response))
    
    return {
      "messages": [
        {
          "type": "unstructured",
          "unstructured": {
            "id": "string",
            "text": response['messages'][0]['content'],
            "timestamp": "string"
          }
        }
      ]
    }
    
