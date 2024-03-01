from producer_interface import mqProducerInterface
import pika
import os

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to class variables
        self.routKey = routing_key
        self.exchangeName = exchange_name
        self.con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters= self.con_params)
        self.channel = self.connection.channel()
        # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service

        # Establish Channel

        # Create the exchange if not already present
        exchange = self.channel.exchange_declare(exchange=self.exchangeName)

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange= self.exchangeName,
            routing_key= self.routKey,
            body= message,
            )
        # Close Channel
        # Close Connection
        self.channel.close()
        self.connection.close()