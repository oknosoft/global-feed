
import {Client} from 'pg';
//import os from 'node:os';

export const sleepTimeout = 6;
export function sleep(timeout) {
  return new Promise((resolve) => {
    setTimeout(resolve, timeout || sleepTimeout);
  });
}

/**
 * @summary Формат даты для логов
 */
export function formatDate(date) {
  if(!date) {
    date = new Date();
  }
  const diff = -date.getTimezoneOffset()/60;
  date.setHours(date.getHours() + diff);
  return date.toISOString().substring(0, 19);
}

/**
 * @summary Читает дату из строки лога
 */
export function reformatDate(str) {
  if(!str.includes('-')) {
    str = `${str.substring(0,4)}-${str.substring(4,6)}-${str.substring(6)}`;
  }
  return new Date(str);
}

/**
 * @summary Интерфейс к Postgres
 */
export class Postgres {

  constructor(withEmitter) {
    const {env} = process;
    this.client = new Client({
      user: env.PGUSER,
      host: env.PGHOST,
      password: env.PGPASSWORD,
      database: 'feed',
    });
    this.busy = false;

    if(withEmitter) {
      import('./emitter.js').then(({Emitter}) => {
        this.emitter = new Emitter();
      });
    }
  }

  /**
   * @summary Выполняет произвольный запрос
   * @desc Ждёт окончания предыдущего запроса, если клиент занят
   * @param {String} sql - Текс запроса
   * @param {Array.<*>} [values] - Массив параметров
   * @return {Promise<*>}
   */
  query(sql, values) {
    const {client, connected} = this;
    const pre = connected ?
      Promise.resolve() :
      client.connect().then(() => this.connected = true);

    return pre.then(async () => {
      while(this.busy) {
        await sleep();
      }
      this.busy = true;
      return client.query(sql, values);
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

  /**
   * @summary Устанавливает значение в таблице параметров по ключу
   * @param {String} name - Ключ параметра
   * @param {*} value - Значение, которое можно сериализовать в JSON
   */
  set(name, value) {
    return this.query(`INSERT INTO settings (param, value) VALUES ('${name}', '${JSON.stringify(value)}')
      ON CONFLICT (param) DO UPDATE SET value = EXCLUDED.value;`);
  }

  /**
   * @summary Читает значение из таблицы параметров
   * @param {String} name - Ключ параметра
   * @return {Promise<*>}
   */
  get(name) {
    return this.query(`select value from settings where param = '${name}';`)
      .then(({rows}) => rows.length ? rows[0].value : '');
  }

  async servers({year, abonent}) {
    const {rows} = await this.query(
      `SELECT branch, addr FROM public.servers where year = ${year} and abonent = ${abonent};`);
    return rows;
  }

  async since(db){
    const {name} = db;
    return (await this.get(`since:${name}`)) || undefined;
  }

  async setSince(db, seq){
    const {name} = db;
    await this.set(`since:${name}`, seq);
    this.emitter.notify();
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

  async lastRev({type, ref, row}) {
    const tmp = await this.query(
      `SELECT * FROM public.feed where type=$1 and ref=$2 order by type, ref, rev desc limit 1;`, [type, ref]);
    if(row) {
      return tmp.rows?.length ? tmp.rows[0] : null;
    }
    return tmp.rows?.length ? tmp.rows[0].rev : '';
  }

  lastSeq() {
    return this.query('SELECT seq FROM feed ORDER BY seq desc limit 1')
      .then(({rows}) => rows[0].seq);
  }

  async docRow({type, ref, rev, strict}) {
    if(!rev) {
      return this.lastRev({type, ref, row: true});
    }
    const tmp = await this.query(
      `SELECT * FROM public.feed where type=$1 and ref=$2 and rev=$3;`,[type, ref, rev]);
    return tmp.rows?.length ? tmp.rows[0] :
      (strict ? null : this.lastRev({type, ref, row: true}));
  }

  append({year, abonent, branch, type, ref, rev, deleted, partner, department, date}) {
    return this.query(
      `call append($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`,
      [year, abonent, branch, type, ref, rev, deleted, partner, department, date]);
  }

  async stat(db) {
    const {name} = db;
    return await this.get(`stat:${name}`) || {
      start: formatDate(),
      all: 0,
      current: 0,
    };
  }

  setStat(db, stat) {
    const {name} = db;
    return this.set(`stat:${name}`, {...stat, moment: formatDate()});
  }
}

