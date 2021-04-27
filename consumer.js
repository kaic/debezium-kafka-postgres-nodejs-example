require('dotenv').config()
const { Pool } = require("pg")
const Promise = require('bluebird')

const pgPool = new Pool({
  user: process.env.PG_USER,
  database: process.env.PG_DB,
  password: process.env.PG_PASS,
  port: process.env.PG_PORT,
  host: process.env.PG_HOST,
  keepAlive: true,
  statement_timeout: Number(process.env.PG_TIMEOUT),
  max: 1
})
let pgConnection = null

const TABLE_NAME = "BalanceOperations"
const NEW_ID_COLUMN_NAME = "id_bigint"
const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 1000
const UPDATE_ORDER = process.env.UPDATE_ORDER || 'asc'
const CONCURRENCY = Number(process.env.CONCURRENCY) || 100
const runCountQuery = process.env.RUN_COUNT_QUERY === 'true' ? true : false
let count = Number(process.env.COUNT) || 0
let rowsUpdated = 0
let rowsNotUpdated = 0
let processeds = 0

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function run() {

  console.log('WORKER JOB IS STARTING WITH PARAMS', { BATCH_SIZE, CONCURRENCY, COUNT: count, PG_TIMEOUT: Number(process.env.PG_TIMEOUT), RUN_COUNT_QUERY: process.env.RUN_COUNT_QUERY })

  await pgPool
    .query("SELECT NOW() as now")
    .then((_) => console.log('WORKER HAS CONNETED TO POSTGRES'))
    .catch(console.error)

  pgConnection = await pgPool.connect()

  const workerJobTimetaken = "TIME TAKEN BY THE WORKER TO COMPLETE THE JOB"
  console.time(workerJobTimetaken)

  if (runCountQuery) {
    const { rows } = await pgConnection.query(
      `SELECT COUNT(1) from "${TABLE_NAME}" where ${NEW_ID_COLUMN_NAME} is null`
    )
    count = Number(rows[0].count)
  }

  console.log(`WORKER HAS ${count} ROWS TO UPDATED`)

  await delay(1000)

  while (processeds < count) {
    await processBatch()
  }

  console.timeEnd(workerJobTimetaken)
  console.log(`WORKER JOB FINISHED`, {
    rowsUpdated,
    rowsNotUpdated
  })

  process.exit(0)
}

async function processBatch(retryAttempt = 0) {
  const batchNumber = processeds / BATCH_SIZE

  if (retryAttempt > 3) {
    console.log(`BATCH ${batchNumber} EXCEEDED RETRY ATTEMPTS - SKIPPING`)
    return
  }

  try {
    const { rows } = await pgConnection.query(`SELECT * from "${TABLE_NAME}" where ${NEW_ID_COLUMN_NAME} is null order by id ${UPDATE_ORDER} LIMIT ${BATCH_SIZE}`)

    await Promise.map(rows, updateRow, {
      concurrency: CONCURRENCY
    })

  } catch (error) {
    console.log(`ERROR WHEN PROCESSING BATCH ${batchNumber}`, error)
    console.log(`RETRYING PROCESSING FOR BATCH ${batchNumber}`)

    retryAttempt++
    await delay(100)
    await processBatch(retryAttempt)
  }
}

async function updateRow(params) {
  const row = params.row || params
  const retryAttempt = params.retryAttempt || 0

  if (retryAttempt > 3) {
    console.log(`ROW ${row.id} EXCEEDED RETRY ATTEMPTS, SKIPPING | NOT UPDATEDS - [${count}/${rowsNotUpdated}]`)
    rowsNotUpdated++
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
    console.log(`SUCCESS ON PROCESSING ROW ID ${row.id} ON ATTEMPT ${retryAttempt} | UPDATEDS [${count}/${rowsUpdated}]`)

  } catch (error) {
    await pgConnection.query("ROLLBACK")
    console.log(`ERROR ON PROCESSING ROW ID ${row.id} - RETRYING`)
    retryAttempt++
    await delay(1000)
    await updateRow({ row, retryAttempt })
  }

  processeds++

}

(async () => await run())()
