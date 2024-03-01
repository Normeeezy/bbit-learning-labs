from consumer_interface import mqConsumerInterface
import pika
import os 

class mqConsumer(mqConsumerInterface): 
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ):
        self.bindingKey = binding_key
        self.exchangeName = exchange_name
        self.queueName = queue_name
        self.setupRMQConnection()
        self.channel.queue_declare(queue=self.queueName)
        self.exchange = self.channel.exchange_declare(exchange= self.exchangeName)

    def setupRMQConnection(self):
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = connection.channel()

        # Create Queue if not already present
        

        # Create the exchange if not already present

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue= self.queueName,
            routing_key= self.bindingKey,
            exchange = self.exchangeName,
            )
        
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            self.queueName, self.on_message_callback, auto_ack=False
            )

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ):
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)

        #Print message (The message is contained in the body parameter variable)
        print(body)


    def startConsuming(self):
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        self.channel.start_consuming()

    def __del__(self):
        print("Closing RMQ connection on destruction")

        # Close Channel
        # Close Connection
        self.channel.close()
        self.connection.close()