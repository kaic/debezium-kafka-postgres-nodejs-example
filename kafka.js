const { Kafka } = require('kafkajs')

const setupKafka = (config) => {
  const ssl = config.ssl ?
    { ssl: config.ssl } : {}

  const endpoints = config.endpoints.split(',')

  const kafka = new Kafka({
    brokers: endpoints,
    ...ssl,
    retry: {
      maxRetryTime: config.max_retry_time || 30000,
      initialRetryTime: config.initial_retry_time || 300,
      retries: config.retries || 5,
    },
  })

  return kafka
}

module.exports = setupKafka
