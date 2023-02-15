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
});

const consumer = kafka.consumer({ groupId: 'kafka-js' });

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC, fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
};

run().catch(console.error);
