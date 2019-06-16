#!/usr/bin/env python
import json
import sqlite3
from sqlite3 import Error
import pika
import taskHandler as taskHandler


def callback(ch, method, properties, body):
    msg = json.loads(body)
    path = msg['dbpath']
    country = msg['country']
    year = msg['year']
    conn = create_connection(path)
    run_task_1(conn)
    run_task_2(conn)
    run_task_3(conn)
    run_task_5(conn)
    conn.close()


def initRabbitMQ (queueName):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queueName)
    channel.basic_consume(queue=queueName, auto_ack=True, on_message_callback=callback)
    channel.start_consuming()

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)

def run_task_1(conn):
    taskHandler.task1(conn)

def run_task_2(conn):
    taskHandler.task2(conn)

def run_task_3(conn):
    taskHandler.task3(conn)

def run_task_5(conn):
    taskHandler.task5(conn)

def start(queueName):
    initRabbitMQ(queueName)

start('messageQueue')