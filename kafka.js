// KafkaJS: https://github.com/tulios/kafkajs
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'kafkaJS',
  brokers: process.env.KAFKA_BOOTSTRAP_SERVERS.split(','),
  ssl: true,
  sasl: {
    mechanism: 'scram-sha-512',
    username: process.env.KAFKA_SASL_USERNAME,
    password: process.env.KAFKA_SASL_PASSWORD
  },
})

const consumer = kafka.consumer({ groupId: `kafka-js-${Date.now()}` });
const producer = kafka.producer();

const pipePetsSpending = async (message) => {
  const jsonMessage = JSON.parse(message.value.toString())

  if (jsonMessage['name'] === 'get_account_transactions') {
    jsonMessage['input_blob'].forEach(async (item) => {
      if (item['transaction_classification'].includes('Pets') && item.amount < 0) {

        await producer.send({
          topic: 'bank_connections_demo',
          messages: [
            {
              value: JSON.stringify({
                transaction_classification: 'Pets',
                amount: Math.abs(item.amount)
              })
            },
          ],
        })
      }
    })
  }
}

const run = async () => {
  await consumer.connect()
  await producer.connect()

  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ _topic, _partition, message }) => await pipePetsSpending(message),
  })
}

run().catch(console.error)
