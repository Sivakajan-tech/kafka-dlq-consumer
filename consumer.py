import os
import socket
import logging
from confluent_kafka import Consumer,  KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

logger = logging.getLogger(__name__)

class KafkaConsumerClass:
    def __init__(self, topic, group_id, bootstrap_servers):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.thread = None

    def oauth_cb(self, oauth_config):
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("us-east-1")
        return auth_token, expiry_ms/1000

    def start(self, num_messages):
        if not self.consumer:
            # Configure the consumer using Confluent Kafka
            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',  # Start reading from the earliest message
                # 'enable.auto.commit': False,  # Disable auto-commit
                'sasl.mechanism': 'OAUTHBEARER',  # SASL Mechanism for authentication
                'oauth_cb': self.oauth_cb,
                'security.protocol': 'SASL_SSL',  # SSL security with SASL authentication
                # 'api.version.request': True,  # Request API version to handle brokers automatically
                'client.id': socket.gethostname(),  # Use the hostname of the machine as client id
                'debug': "all"
            }
            
            # Initialize the consumer
            self.consumer = Consumer(conf)
            self.consumer.subscribe([self.topic])
            logger.info(f"Received data: { data["message"]["id"] }")
            
        # Start consuming messages
        self._consume_messages(num_messages)
        
    def _consume_messages(self, num_messages):
        try:
            processed_count = 0
            while processed_count < num_messages:
                msg = self.consumer.poll(1.0)  # Poll for messages with a 1-second timeout
                if msg is None:
                    continue  # No message available, continue polling
                elif msg.error():
                    # Handle errors if there are any
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.error(f"End of partition reached {msg.partition}, offset {msg.offset()}")
                    else:
                        logger.error(f"Error occurred while consuming the message, error code {msg.error()}")
                        os._exit(1)
                else:
                    # Successfully received a message
                    logger.info(f"Received message {msg.value()} : offset  {msg.offset()}")
                    print(f"Received message {msg.value()} : offset  {msg.offset()}")
                    processed_count += 1
                    
                    # Manually commit the offset after processing
                    self.consumer.commit(asynchronous=False)
        except Exception as ex:
            logger.error(f"Error occurred while consuming message, {str(ex)}. Terminating the consumer.", exc_info=True)
            os._exit(1)

    def stop(self):
        if self.consumer:
            self.consumer.close()  # Close the consumer connection
    
if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    group_id = os.getenv("KAFKA_GROUP_ID", "")
    dlq_topic = os.getenv("KAFKA_TOPIC","")
    message_read_count = os.getenv("MESSAGE_READ_COUNT")

    if not message_read_count:
        logger.warning("MESSAGE_READ_COUNT is not set. Exiting.")
        os._exit(1)
    num_messages = int(message_read_count)
    
    logger.info(f"Kafka server is going to listen on Server: {bootstrap_servers}, topic: {dlq_topic}, group id: {group_id}")
    logger.info(f"Number of messages to consume: {num_messages}")
    consumer = KafkaConsumerClass(dlq_topic, group_id, bootstrap_servers)
    consumer.start(num_messages)
