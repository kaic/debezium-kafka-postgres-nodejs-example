require('dotenv').config()
const { Pool } = require("pg");

const postgresPool = new Pool({
  user: process.env.PG_USER,
  database: process.env.PG_DB,
  password: process.env.PG_PASS,
  port: process.env.PG_PORT,
  host: process.env.PG_HOST,
  keepAlive: true,
  max: 60,
});

const TABLE_NAME = "BalanceOperations"
const NEW_ID_COLUMN_NAME = "id_bigint"
const BATCH_SIZE = Number(process.env.BATCH_SIZE) || 100
let offset = Number(process.env.OFFSET) || 0
let count = 0
let sum = 0

async function run () {

  console.log('WORKER JOB IS STARTING')

  await postgresPool
    .query("SELECT NOW() as now")
    .then((_) => console.log('WORKER HAS CONNETED TO POSTGRES'))
    .catch(console.error);

  const timetaken = "TIME TAKEN BY THE WORKER TO COMPLETE THE JOB";
  console.time(timetaken);

  const postgresConnection = await postgresPool.connect();  

  // const { rows } = await postgresConnection.query(
  //   `SELECT COUNT(1) from "${TABLE_NAME}" where ${NEW_ID_COLUMN_NAME} is null`
  // );

  // count = rows[0].count

  // console.log(`WORKER HAS ${count} ROWS TO UPDATED`)
  
  let batch = []
   do {
    const { rows } = await postgresConnection.query(
      `SELECT * from "${TABLE_NAME}" where ${NEW_ID_COLUMN_NAME} is null order by id asc LIMIT ${BATCH_SIZE} OFFSET ${offset}`
    );

    batch = rows
    
    await batch.map(updateRow)
    offset += BATCH_SIZE
    console.log({offset, BATCH_SIZE})
  } while (offset <  count)

  console.timeEnd(timetaken);
  console.log(`WORKER JOB FINISHED`)
}

async function updateRow(row) {
  const postgresConnection = await postgresPool.connect();

  try {
    if(!row.id_bigint) {
      await postgresConnection.query("BEGIN")

      const {
        rows,
      } = await postgresConnection.query(
        `UPDATE "${TABLE_NAME}" set ${NEW_ID_COLUMN_NAME} = $1 WHERE id = $1 RETURNING *`,
        [row.id]
      );
      
      sum++
      console.log(`[${count}/${sum}] UPDATED ROWS - ID ${row.id}`)
      await postgresConnection.query("COMMIT");
    }
  } catch (e) {
    await postgresConnection.query("ROLLBACK");
    throw e;
  } finally {
    await postgresConnection.release();
  }
}

(() => run())();
