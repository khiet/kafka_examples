# https://github.com/zendesk/ruby-kafka
require 'kafka'

kafka = Kafka.new(
  ENV.fetch('KAFKA_BOOTSTRAP_SERVERS').split(','),
  sasl_scram_username: ENV.fetch('KAFKA_SASL_USERNAME'),
  sasl_scram_password: ENV.fetch('KAFKA_SASL_PASSWORD'),
  sasl_scram_mechanism: 'sha512',
  ssl_ca_certs_from_system: true,
  sasl_over_ssl: true
)

# Consume

consumer = kafka.consumer(group_id: 'ruby-kafka')
consumer.subscribe(ENV.fetch('KAFKA_TOPIC'))

consumer.each_message do |message|
  puts message.topic, message.partition
  puts message.offset, message.key, message.value
end

# Produce

# require 'json'
# producer = kafka.producer
# producer.produce({ ping: Time.new }.to_json, topic: "example")
# producer.deliver_messages
