from producer_interface import mqProducerInterface

import os
import pika

class mqProducer(mqProducerInterface):

    def __init__(self, routing_key, exchange_name) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()













