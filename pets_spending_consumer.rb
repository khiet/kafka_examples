# https://github.com/zendesk/ruby-kafka
require 'kafka'

# 1. Consume from the 'pets_spending' topic
# 2. For each message, display different types of cows 🐮 depending on the following use cases:
#   case 1: amount < 30  => cow
#   case 2: amount >= 30 => cow with tongue
#   case 3: amount >= 50 => rainbow cow with tongue
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
consumer.subscribe('pets_spending')

system 'clear'
cowsay("Feed me!")

consumer.each_message do |message|
  parsed_message = JSON.parse(message.value)
  amount = parsed_message['amount']

  system 'clear'
  cowsay("Thank you! £#{amount}", tongue: amount >= 30, color: amount >= 50)

  sleep 0.1
end
