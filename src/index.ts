const oracledb = require('oracledb');
const { Client } = require('pg')
//  import {default as oracledb} from "oracledb";

import {QueryResult} from 'pg';

async function getOracleConnection() {
  return await oracledb.getConnection({
    user          : "system",
    password      : "285628kL",
    connectString : "(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.2.10)(PORT = 1521))(CONNECT_DATA =(SID= ORCL)))"
  });
}

async function getOracleTables(conn: any) {

  return await conn.execute(
    `SELECT DISTINCT OWNER, OBJECT_NAME 
FROM DBA_OBJECTS
WHERE OBJECT_TYPE = 'TABLE'
AND OWNER = 'GAEE2020' ORDER BY OBJECT_NAME`);
}





function getPGConnection() {
  const client = new Client({
    user: 'mperdikeas',
    host: '192.168.2.2',
    database: 'changeme',
    password: 'changeme',
    port: 5432,
  })
  client.connect()
  return client;
}

async function getPGTables(conn: any): Promise<QueryResult<{schema_name: string, table_name: string}>> {
  //@ts-ignore
  const query_1 = `SELECT DISTINCT table_name
FROM information_schema.tables
WHERE table_schema = 'gaee2020' ORDER BY table_name`;

  // https://stackoverflow.com/a/58244497/274677
  const query_2 = `
WITH 
    partition_parents AS (
        SELECT
            relnamespace::regnamespace::text AS schema_name,
            relname                          AS table_name
        FROM pg_class
        WHERE relkind = 'p'), -- The parent table is relkind 'p', the partitions are regular tables, relkind 'r'

    unpartitioned_tables AS (     
        SELECT
            relnamespace::regnamespace::text AS schema_name,
            relname                          AS table_name
        FROM pg_class
        WHERE relkind = 'r'
        AND NOT relispartition
    ) -- Regular table
SELECT * FROM partition_parents 
WHERE schema_name in ('gaee2020')
UNION
SELECT * FROM unpartitioned_tables
WHERE schema_name in ('gaee2020')
order by 1,2`;

  const rv = await conn.query(query_2);
  return rv;
}

async function doWork() {
  let ora_conn;
  let pg_conn;
  try {

    oracledb.initOracleClient();
    ora_conn = await getOracleConnection();
    const ora_tables = await getOracleTables(ora_conn);
    console.log('oracle tables are: ', ora_tables);


    pg_conn = getPGConnection();
    const tables = await getPGTables(pg_conn);
    console.log('getPGTables returnred: ', tables);

  } finally {
    if (ora_conn) {
      try {
        await ora_conn.close();
      } catch (err: any) {
        if (err.message) {
          console.error('failed to close the ORACL connecton');
          console.error(err.message);
        }
      }
      try {
        await pg_conn.end();
      } catch (err: any) {
        if (err.message) {
          console.error('failed to close the PG connecton');
          console.error(err.message);
        }
      }      
    }
  }
}


doWork();





