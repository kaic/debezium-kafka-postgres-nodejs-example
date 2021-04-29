require('dotenv').config()
const { Pool } = require("pg")
const Promise = require('bluebird')
const { setupKafka } = require("./kafka")

const pgPool = new Pool({
  keepAlive: true,
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  port: process.env.PG_PORT,
  database: process.env.PG_DB,
  password: process.env.PG_PASS,
  max: 10,
  statement_timeout: Number(process.env.PG_TIMEOUT),
})
let pgConnection = null

let rowsUpdated = 0
let rowsNotUpdated = 0
let rowsAlreadyUpdated = 0

const TABLE_NAME = "BalanceOperations"
const NEW_ID_COLUMN_NAME = "id_bigint"
const runCountQuery = process.env.RUN_COUNT_QUERY === 'true' ? true : false

async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function updateRow(row, retryAttempt = 0) {

  if (retryAttempt > 3) {
    rowsNotUpdated++
    console.log(`ROW ${row.id} EXCEEDED RETRY ATTEMPTS, SKIPPING | [${rowsNotUpdated} ROWS NOT UPDATED]`)
    return
  }

  if (row.id_bigint) {
    rowsAlreadyUpdated++
    console.log(`ROW ID ${row.id} ALREADY UPDATED | [${rowsAlreadyUpdated} ROWS ALREADY UPDATED]`)
    return
  }

  try {
    await pgConnection.query("BEGIN")
    await pgConnection.query(
      `UPDATE "${TABLE_NAME}" set ${NEW_ID_COLUMN_NAME} = $1 WHERE id = $1`,
      [row.id]
    )

    await pgConnection.query("COMMIT")
    rowsUpdated++
    console.log(`SUCCESS ON PROCESSING ROW ID ${row.id} ON ATTEMPT ${retryAttempt} | [${rowsUpdated} ROWS UPDATED]`)

  } catch (error) {
    await pgConnection.query("ROLLBACK")

    console.log(`ERROR ON PROCESSING ROW ID ${row.id} - RETRYING`)
    await delay(100)
    await updateRow(retryAttempt++)
  }
}

async function run() {
  console.log('WORKER JOB IS STARTING WITH PARAMS', { PG_TIMEOUT: Number(process.env.PG_TIMEOUT), RUN_COUNT_QUERY: process.env.RUN_COUNT_QUERY })

  pgConnection = await pgPool.connect()

  await pgPool
    .query("SELECT NOW() as now")
    .then((_) => console.log('WORKER HAS CONNETED TO POSTGRES'))
    .catch(console.error)

  const kafkaConfig = {
    ssl: true,
    brokers: process.env.BROKERS
  }
  const kafka = setupKafka(kafkaConfig)
  const consumer = kafka.consumer({ groupId: "kafka-connect" })
  await consumer.connect()
  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC })

  console.log(`WORKER HAS SUBSCRIBE TO ${process.env.KAFKA_TOPIC}`)

  if (runCountQuery) {
    const { rows } = await pgConnection.query(
      `SELECT COUNT(1) from "${TABLE_NAME}" where ${NEW_ID_COLUMN_NAME} is null`
    )
    const count = Number(rows[0].count)

    console.log(`WORKER HAS ${count} ROWS TO UPDATED`)
  }

  await delay(1000)

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const parsedMessage = JSON.parse(message.value.toString())

      const row = parsedMessage.after
      console.log(`MESSAGE INCOMING FOR ROW ${row.id}`)
      await updateRow(row)

    }
  })
}

(async () => run())()
