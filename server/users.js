
import {Couchdb} from '../listener/couchdb.js';
import {branchesOrder} from '../listener/branches.js';

class UsersManager {

  #rows = [];

  constructor(rows, abonents) {
    for(const {doc: {_id, _rev, timestamp, acl, captured, ...doc}} of rows) {
      //const abonent = abonents.find(v => v.ref === owner);
      //const branch = {ref, id: parseInt(suffix), suffix, owner: abonent, parent, name, server, children: []};
      if(doc.subscribers) {
        doc.ref = _id.substring(10);
        const branch = abonents.byRef(doc.branch);
        this.#rows.push(doc);
      }
    }
  }

}

export async function loadUsers() {
  const {DBUSER, DBPWD, COUCHLOCAL, ZONE = 10} = process.env;
  const db = new Couchdb(COUCHLOCAL, {auth: {username: DBUSER, password: DBPWD}});
  const res = await db.fetch(`/wb_${ZONE}_ram/_all_docs?start_key="cat.users|"&end_key="cat.users|z"&include_docs=true`);
  const abonents = await branchesOrder();
  return new UsersManager(res.rows, abonents);
}
