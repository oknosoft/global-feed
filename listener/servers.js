
import {Couchdb, nil} from './couchdb.js';

const year = 2026;
const sql = `INSERT INTO servers(year, abonent, branch, addr) VALUES ($1, $2, $3, $4)
ON CONFLICT (year, abonent, branch) DO UPDATE SET addr = EXCLUDED.addr;`

function addr(server) {
  return (server.http_local || server.http).replace('/wb_', '');
}

/**
 * @summary Освежает таблицу баз текущего года
 * @param {Postgres} postgres
 * @param {BranchesOrder} branches
 * @return {Promise<void>}
 */
export async function currentServers(postgres, branches) {
  const {DBUSER, DBPWD, COUCHLOCAL} = process.env;
  const db = new Couchdb(COUCHLOCAL, {auth: {username: DBUSER, password: DBPWD}});
  const res = await db.fetch('/wb_meta/_all_docs?start_key="cat.servers|"&end_key="cat.servers|z"&include_docs=true');
  const servers = {};
  for(const {doc: {_id, _rev, timestamp, ...doc}} of res.rows) {
    const ref = _id.substring(12);
    servers[ref] = doc;
  }
  for(const abonent of branches.abonents) {
    const server = servers[abonent.server];
    if(server) {
      await postgres.query(sql, [year, abonent.id, 0, addr(server)]);
      for(const branch of abonent.branches) {
        if(/^\d{4}$/.test(branch.suffix)) {
          const server = servers[branch.server || branch.parent.server || abonent.server];
          if(server) {
            await postgres.query(sql, [year, abonent.id, branch.id, addr(server)]);
          }
        }
      }
    }
  }
}
