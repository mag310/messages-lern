import pika
import os
import sys
import json
import syslog
import time
import re
import requests
import telegram
from threading import Thread

class Queue(Thread):

    def __init__(self, queue_name):
        Thread.__init__(self)
        self.queue = queue_name
        self.subscriber = []

    def add_subscriber(self, callback):
        self.subscriber.append(callback)

    def run(self):

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
            channel = connection.channel()

            channel.queue_declare(queue=self.queue, durable=True)
            channel.basic_consume(self.callback,
                                  queue=self.queue,
                                  no_ack=False, exclusive=False)
            channel.basic_qos(prefetch_count=1)
            channel.start_consuming()

        except Exception as exc:
            syslog.syslog("Error while consuming %s queue: %s" % (self.queue, str(exc)))
            connection.close()
            sys.exit(1)

        connection.close()

    def callback(self, ch, method, properties, body):
        res = True
        for sub in self.subscriber:
            res = res and sub.run(body)

        if res :
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_nack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    syslog.openlog('some_tag', syslog.LOG_PID, syslog.LOG_NOTICE)

    threads = []

    try:
        queue_list = [
            'queue_one',
            'queue_two',
            'queue_three',
            'queue_mail',
            'queue_sms',
            'queue_tlgrm',
            'queue_Tgm_1',
        ]

        """
        Инициализируем
        """
        for name in queue_list:
            thread = Queue(name)
            threads.append(thread)

        """
        Место для установки обработчиков
        """

        """
        Стартуем
        """
        for thread in threads:
            thread.start()

    except KeyboardInterrupt:
        print("EXIT")
        raise
