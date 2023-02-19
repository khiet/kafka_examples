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

def cowsay(message, tongue: false, color: false)
  command = tongue ? "cowsay -T U #{message}" : "cowsay #{message}"
  command = "#{command} | lolcat" if color

  system command
end

# Consume

consumer = kafka.consumer(group_id: "ruby-kafka-#{Time.now.to_i}")
consumer.subscribe(ENV.fetch('KAFKA_TOPIC'))

pets_spending = 0
consumer.each_message do |message|
  parsed_message = JSON.parse(message.value)
  pets_spending += parsed_message['amount']

  system 'clear'
  cowsay(pets_spending, tongue: pets_spending > 100, color: pets_spending > 200)

  sleep 0.1
end

# Produce

# require 'json'
# producer = kafka.producer
# producer.produce({ ping: Time.new }.to_json, topic: "example")
# producer.deliver_messages
