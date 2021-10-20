import paho.mqtt.client as mqtt
from elasticsearch import Elasticsearch
import uuid

elastic = Elasticsearch(
    ['search-curso-lxk7wjonxaq3rr6meyoloem4ja.us-east-2.es.amazonaws.com'],
    http_auth=('escritura', 'XIy6DE85NkdYQwzwSK#U'),
    scheme="https",
    port=443,
    )

def on_connect(client, userdata, flags, rc):
    print("Conectado con codigo {0}".format(str(rc)))  # Printamos el resultado de la conexion
    client.subscribe("mqtt/alfonsodiez")  # Nos subscribimos al topico donde estemos publicando mensajes


def on_message(client, userdata, msg): 
    mensaje = msg.payload.decode("utf-8")
    print("Mensaje recibido-> " + msg.topic + " " + str(mensaje))  # Printamos el mensaje recibido en la consola
    datos = str(mensaje).split(',') #el mensaje esperado es del tipo csv(comma separated values) 'millis,x,y,z,potenciometro'
    document = {
        "millis":int(datos[0]),
        "IMU": {
            'x': float(datos[1]),
            'y': float(datos[2]),
            'z': float(datos[3])
        },
        "potenciometro": int(datos[4]),
    }
    
    response = elastic.index(
        index = msg.topic.replace('/','_'),
        doc_type = 'sensorData',
        id = uuid.uuid4(),
        body = document
    )


client = mqtt.Client("consumidor-python")  # Creamos una instancia
client.on_connect = on_connect  # Definimos la funcion que se ejecutara durante la conexion
client.on_message = on_message  # Definimos la funcion que actuara como receptora de mensajes
client.connect('13.59.37.99', 1883)
client.loop_forever()  # iniciamos un loop a la espera de mensajes
