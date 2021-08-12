from google.cloud import firestore
from datetime import datetime

client = firestore.Client(project='python-pubsub-firestore')

def pubsub_fire(event, context):
    import base64

    print(f'This function was triggered by messageId {context.event_id}, published at {context.timestamp} to {context.resource["name"]}!')

    message = ''
    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
    print(f'message: {message}')

    temperature = -1
    humidity = -1
    try:
        if 'attributes' in event:
            attributes = event['attributes']
            temperature = attributes['temperature']
            humidity = attributes['humidity']

            print(f'temperature = {temperature}')
            print(f'humidity = {humidity}')
    except Exception as e:
        print(f'error with attributes: {e}')

    doc_id = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    doc = client.collection('factorySensors').document(doc_id)
    doc.set({
        'message': message,
        'temperature': temperature,
        'humidity': humidity,
    })