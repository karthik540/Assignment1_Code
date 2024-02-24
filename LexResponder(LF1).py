import json
import os
import logging, boto3
logger = logging.getLogger()
logger.setLevel("INFO")

def delegate_intent(intent, slots, response_message):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Delegate"
            },
            "intent": {
                "name": intent,
                "slots" : slots
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": response_message
            }
        ],
    }


def done_intent(intent, response_message):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": intent,
                "state": "Fulfilled"
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": response_message
            }
        ],
    }

def elicit_intent(intent, response_message, slots, elicit_slot):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": elicit_slot,
            },
            "intent": {
                "name": intent,
                "slots": slots
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": response_message
            }
        ],
    }

def push_queue(slots):
    
    message = {
        'Location' : slots['Location']['value']['interpretedValue'],
        'Cuisine' : slots['Cuisine']['value']['interpretedValue'],
        'NumberOfPeople' : slots['NumberOfPeople']['value']['interpretedValue'],
        'DiningTime' : slots['DiningTime']['value']['interpretedValue'],
        'Email' : slots['Email']['value']['interpretedValue']
    }
    
    logger.info(message)
    
    
    sqs_client = boto3.client('sqs')
    sqs_client.send_message(
        QueueUrl = "https://sqs.us-east-1.amazonaws.com/665899358319/SuggestionRequestQueue",
        MessageBody = str(message)
    )
    
    logger.info("Message Pushed !")
    

def validate_slots(slots):
    isValid = True
    elicitSlot = ""
    message = ""
    
    if slots['Location'] == None:
        isValid = False
        elicitSlot = "Location"
        message = "Which Location would you like to get suggestion ?"
    elif slots['Cuisine'] == None:
        isValid = False
        elicitSlot = "Cuisine"
        message = "What Cuisine ?"
    elif slots['NumberOfPeople'] == None:
        isValid = False
        elicitSlot = "NumberOfPeople"
        message = "How many Number of People ?"
    elif slots['DiningTime'] == None:
        isValid = False
        elicitSlot = "DiningTime"
        message = "What Dining Time ?"
    elif slots['Email'] == None:
        isValid = False
        elicitSlot = "Email"
        message = "To what email id shall I send suggestion ?"
    
    return {
        "isValid": isValid,
        "elicitSlot": elicitSlot,
        "message": message
    }    


def lambda_handler(event, context):
    # TODO implement'
    logger.info(event)
    logger.info(context)
    '''
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }'''
    
    intent = event['sessionState']['intent']['name']
    slots = event['sessionState']['intent']['slots']
    invocation_source = event['invocationSource']
    
    logger.info(intent)
    logger.info(slots)
    logger.info(invocation_source)
    
    if invocation_source == 'DialogCodeHook':
        if intent == 'GreetingIntent':
            message = "Hi ! How can i help you ?"
            return done_intent(intent, message)
        elif intent == 'ThankYouIntent':
            message = "Welcome ! Hope you liked the service !"
            return done_intent(intent, message)
        elif intent == 'DiningSuggestionsIntent':
            logger.info("Enters DiningSuggestionsIntent")
            if invocation_source == 'DialogCodeHook':
                logger.info("Enters DialogCodeHook")
                result = validate_slots(slots)
                logger.info(result)
                
                if result['isValid'] == False:
                    return elicit_intent(intent, result['message'], slots , result['elicitSlot'])
                else:
                    message = "All slots are ok !"
                    logger.info("Slots filled !")
                    logger.info(slots)
                    #push_queue(slots)
                    return delegate_intent(intent, slots, message)
                    #return done_intent(intent, message)
            else:
                message = "Non DialogCodeHook !"
                return done_intent(intent, message)
    elif invocation_source == 'FulfillmentCodeHook':
        message = "Your reservation has been placed"
        push_queue(slots)
        logger.info("Pushed to Queue")
        return done_intent(intent, message)
        
    message = "Invalid Message Intent !"
    return done_intent(intent, message)
    
    
