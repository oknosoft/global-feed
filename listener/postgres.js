
import {Client} from 'pg';

function sleep() {
  return new Promise((resolve) => {
    setTimeout(resolve, 8);
  });
}

export class Postgres {

  constructor() {
    const {env} = process;
    this.client = new Client({
      user: env.PGUSER,
      host: env.PGHOST,
      password: env.PGPASSWORD,
      database: 'feed',
    });
    this.busy = false;
  }

  query(sql, ...params) {
    const {client, connected} = this;
    const pre = connected ?
      new Promise(async (resolve) => {
        while(this.busy) {
          await sleep();
        }
        resolve();
      }) :
      client.connect().then(() => this.connected = true);

    return pre.then(async () => {
      while(this.busy) {
        await sleep();
      }
      this.busy = true;
      return client.query(sql, ...params);
    })
      .then((res) => {
        this.busy = false;
        return res;
      })
      .catch(err => {
        this.busy = false;
        throw err;
      });
  }

  set(name, value) {
    return this.query(`INSERT INTO settings (param, value) VALUES ('${name}', '${JSON.stringify(value)}')
      ON CONFLICT (param) DO UPDATE SET value = EXCLUDED.value;`);
  }

  get(name) {
    return this.query(`select value from settings where param = '${name}';`)
      .then(({rows}) => rows.length ? rows[0].value : '');
  }

  async servers({year, abonent}) {
    const {rows} = await this.query(
      `SELECT branch, addr FROM public.servers where year = ${year} and abonent = ${abonent};`);
    return rows;
  }

  async setServers({yerar, abonent, branch, addr}) {

  }

  async since(db){
    const {name} = db;
    return (await this.get(`since:${name}`)) || undefined;
  }

  async setSince(db, seq){
    const {name} = db;
    return await this.set(`since:${name}`, seq);
  }

  async exists({type, ref, rev}, strict) {
    let tmp = await this.query(
      `SELECT exists (SELECT 1 FROM feed  WHERE type=$1 and ref=$2 and rev=$3);`,
      [type, ref, rev]);
    if(tmp.rows[0]?.exists) {
      return strict ? true : {ref: true, rev: true};
    }
    if(strict) {
      return false;
    }
    tmp = await this.query(
      `SELECT exists (SELECT 1 FROM feed  WHERE type=$1 and ref=$2);`,
      [type, ref]);
    return {ref: tmp.rows[0]?.exists, rev: false};
  }

  async lastRev({type, ref}) {
    const tmp = await this.query(
      `SELECT rev FROM public.feed where type=$1 and ref=$2 limit 1);`,
      [type, ref]);
    return tmp.rows?.length ? tmp.rows[0].rev : '';
  }

  append({year, abonent, branch, type, ref, rev, deleted, partner, department, date}) {
    return this.query(
      `call append($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`,
      [year, abonent, branch, type, ref, rev, deleted, partner, department, date]);
  }

  async stat(db) {
    const {name} = db;
    return await this.get(`stat:${name}`) || {
      start: new Date().toISOString().substring(0, 19),
      doc_count: 0,
    };
  }

  setStat(db, stat) {
    const {name} = db;
    return this.set(`stat:${name}`, stat);
  }
}

