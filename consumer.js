require('dotenv').config()
const { Pool } = require("pg")
const Promise = require('bluebird')

let pgConnection = null
const pgPool = new Pool({
  user: process.env.PG_USER,
  database: process.env.PG_DB,
  password: process.env.PG_PASS,
  port: process.env.PG_PORT,
  host: process.env.PG_HOST,
  keepAlive: true,
  statement_timeout: 300000,
  max: 3
})

const TABLE_NAME = "BalanceOperations"
const NEW_ID_COLUMN_NAME = "id_bigint"
const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 1000
const CONCURRENCY = Number(process.env.CONCURRENCY) || 100
let offset = Number(process.env.OFFSET) || 0
let count = Number(process.env.COUNT) || 0
let rowsUpdated = 0
let rowsNotUpdated = 0

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function run() {

  console.log('WORKER JOB IS STARTING')
  console.log('PARAMS')
  console.log({ TABLE_NAME, NEW_ID_COLUMN_NAME, BATCH_SIZE, CONCURRENCY, OFFSET: process.env.OFFSET, COUNT: process.env.COUNT })

  pgConnection = await pgPool.connect()
  await pgPool
    .query("SELECT NOW() as now")
    .then((_) => console.log('WORKER HAS CONNETED TO POSTGRES'))
    .catch(console.error)


  const workerJobTimetaken = "TIME TAKEN BY THE WORKER TO COMPLETE THE JOB"
  console.time(workerJobTimetaken)

  const { rows } = await pgConnection.query(
    `SELECT COUNT(1) from "${TABLE_NAME}" where ${NEW_ID_COLUMN_NAME} is null`
  )

  count = Number(rows[0].count)

  console.log(`WORKER HAS ${count} ROWS TO UPDATED`)

  await delay(1000)

  //retirar do while
  //salvar offset no banco
  //salvar batchs não processados no banco
  let batchNumber = 0
  while (offset < count) {
    await processBatch(batchNumber, offset)
    batchNumber++
    offset += BATCH_SIZE
    console.log({ offset, count })
  }

  console.timeEnd(workerJobTimetaken)
  console.log(`ROWS NOT UPDATED - ${rowsNotUpdated}`)
  console.log(`ROWS UPDATED - ${rowsUpdated}`)
  console.log(`WORKER JOB FINISHED`)
}

async function processBatch(batchNumber, _offset, retryAttempt = 0) {
  console.log({ batchNumber, _offset, retryAttempt })
  console.time("TIME TAKEN TO PROCESS BATCH")

  if (retryAttempt > 3) {
    console.log(`BATCH ${batchNumber} EXCEEDED RETRY ATTEMPTS - SKIPPING FOR OFFSET ${_offset}`)
    return
  }

  try {
    const { rows } = await pgConnection.query(
      `SELECT * from "${TABLE_NAME}" where ${NEW_ID_COLUMN_NAME} is null order by id asc LIMIT ${BATCH_SIZE} OFFSET ${_offset}`
    )

    await Promise.map(rows, updateRow, {
      concurrency: CONCURRENCY
    })

  } catch (error) {
    console.log(`ERROR WHEN PROCESSING BATCH ${batchNumber}`, error)
    console.log(`RETRYING PROCESSING FOR BATCH ${batchNumber}`)

    retryAttempt++
    await processBatch(batchNumber, _offset, retryAttempt)
  }

  console.timeEnd("TIME TAKEN TO PROCESS BATCH")
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
    if (!row.id_bigint) {
      await pgConnection.query("BEGIN")

      await pgConnection.query(
        `UPDATE "${TABLE_NAME}" set ${NEW_ID_COLUMN_NAME} = $1 WHERE id = $1`,
        [row.id]
      )

      await pgConnection.query("COMMIT")
      rowsUpdated++
      console.log(`SUCCESS ON PROCESSING ROW ID ${row.id} ON ATTEMPT ${retryAttempt} | UPDATEDS [${count}/${rowsUpdated}]`)
    }
  } catch (error) {
    await pgConnection.query("ROLLBACK")
    console.log(`ERROR ON PROCESSING ROW ID ${row.id} - RETRYING`)
    retryAttempt++
    await updateRow({ row, retryAttempt })
  }
}

(async () => await run())()
