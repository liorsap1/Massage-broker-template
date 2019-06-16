#!/usr/bin/env python
import pika
import json
import os

def init(ip):
    connection = pika.BlockingConnection(pika.ConnectionParameters(ip))
    return connection

def initChannel(queueName, connection):
    channel = connection.channel()
    channel.queue_declare(queue=queueName)
    return channel

def start(queueName, country, year):
    path = os.getcwd() + "\sqlite\db\chinook.db"
    message = {
        'dbpath': path,
        'country': country,
        'year': year
    }
    connection = init('localhost')
    channel = initChannel(queueName, connection)
    channel.basic_publish(exchange='', routing_key=queueName, body=json.dumps(message))
    connection.close()

start('messageQueue', 'USA', 2013)