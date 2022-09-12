# Import KafkaProducer from Kafka library
from kafka import KafkaProducer
# Define server with port
bootstrap_servers = ['localhost:9092']
# Define topic name where the message will publish
topicName = 'amazon_user_activities1'
# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
# Publish text in defined topic
producer.send(topicName, b'Welcome to CFAMILYIT...KAFKA WORLD...')
# Print message
print("Message Sent")
producer.flush()
producer.close()