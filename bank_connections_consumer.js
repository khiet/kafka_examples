// KafkaJS: https://github.com/tulios/kafkajs
const { Kafka } = require('kafkajs')

// 1. Subscribe to the 'bank connections' topic
// 2. For each account transaction in 'get_account_transactions' message
// 3.   Look for 'Pets' in 'transaction_classification'
// 4.   Produce a message to the 'pets_spending' topic with the transaction 'amount'
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
          topic: 'pets_spending',
          messages: [
            {
              value: JSON.stringify({ amount: Math.abs(item.amount) })
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

  await consumer.subscribe({ topic: 'bank_connections', fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ _topic, _partition, message }) => await pipePetsSpending(message),
  })
}

run().catch(console.error)
