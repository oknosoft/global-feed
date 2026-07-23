import {Postgres} from '../listener/postgres.js';
import {Couchdb} from '../listener/couchdb.js';

function toServer(name, abonent, branch, auth) {
  let addr = `${name}/wb_${abonent}_doc`;
  if(branch) {
    let suffix = branch.toFixed();
    while (suffix.length < 4) {
      suffix = '0' + suffix;
    }
    addr += `_${suffix}`;
  }
  return new Couchdb(addr, {auth});
}

export class PostgresClient extends Postgres {

  #servers = new Map();

  async init() {
    const {rows} = await this.query(`select * from servers`);
    for(const row of rows) {
      if(!this.#servers.has(row.year)) {
        this.#servers.set(row.year, new Map());
      }
      const year = this.#servers.get(row.year);
      if(!year.has(row.abonent)) {
        year.set(row.abonent, new Map());
      }
      const abonent = year.get(row.abonent);
      abonent.set(row.branch, row.addr);
    }

    const {DBUSER, DBPWD, COUCHLOCAL} = process.env;
    this.auth = {username: DBUSER, password: DBPWD};

    return this;
  }

  async servers({year, abonent, branch}) {
    let direct = this.#servers.get(year).get(abonent).get(branch);
    let root = branch ? this.#servers.get(year).get(abonent).get(0) : direct;
    if(typeof direct === 'string') {
      direct = toServer(direct, abonent, branch, this.auth);
      this.#servers.get(year).get(abonent).set(branch, direct);
    }
    if(typeof root === 'string') {
      if(branch) {
        root = toServer(root, abonent, branch, this.auth);
        this.#servers.get(year).get(abonent).set(0, root);
      }
      else {
        root = direct;
      }
    }
    return {direct, root};
  }

}

