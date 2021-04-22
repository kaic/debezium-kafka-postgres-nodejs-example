const { Pool } = require("pg");
const logger = require('./logger')
const { Kafka } = require("kafkajs");

(async () => {

  logger.info('Worker is starting')

  await setTimeout(() => logger.info('Waiting 10 seconds to start'), 10000);

  const kafka = new Kafka({
    clientId: "consumer-worker",
    brokers: ["kafka:9092"],
  });

  const postgresPool = new Pool({
    user: "",
    database: "",
    password: "",
    port: 5432,
    host: "localhost",
    keepAlive: true,
    max: 10,
  });

  const TABLE_NAME = "BalanceOperations";
  const NEW_ID_COLUMN_NAME = "id_bigint";

  await postgresPool
    .query("SELECT NOW() as now")
    .then((_) => logger.info('Worker has connnected to Postgres'))
    .catch(console.error);

  const consumer = kafka.consumer({ groupId: "consumer-group" });

  await consumer.connect();

  await consumer.subscribe({ topic: "dbserver1.public.BalanceOperations" });

  logger.info('Worker ready to go!')

  const postgresConnection = await postgresPool.connect();

  const { rows, count } = await postgresConnection.query(
    "SELECT COUNT(1) from $1 where $2 is not null",
    [TABLE_NAME, NEW_ID_COLUMN_NAME]
  );

  logger.info(`There are ${count} rows to update`)

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const parsedMessage = JSON.parse(message.value.toString());
      const row = parsedMessage.payload.after;

      logger.info('Message Incoming', parsedMessage)

      const postgresConnection = await postgresPool.connect();

      try {
        await postgresConnection.query("BEGIN");

        await postgresConnection.query(
          "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        );

        const {
          rows,
        } = await postgresConnection.query(
          "UPDATE users set id_bigint = $1 WHERE id = $1 RETURNING *",
          [row.id]
        );

        logger.info(`Row ${row.id} updated`, rows)

        await postgresConnection.query("COMMIT");
      } catch (e) {
        await postgresConnection.query("ROLLBACK");
        throw e;
      } finally {
        postgresConnection.release();
      }
    },
  });
})();
