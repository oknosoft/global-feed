
import {formatDate, reformatDate} from './postgres.js';
import {Couchdb, nil} from './couchdb.js';

const {DBUSER, DBPWD} = process.env;
const interval = 6000;        // задержка между базами на старте и при ошибке
const statInterval = 300000;  // статистика и перезапуск раз в 5 минут
const dateCache = {};             // даты документов
const classNames = /^(doc\.calc_order|cat\.characteristics)/;
const fakeFunc = () => null;

export function log(...args) {
  console.log(formatDate(), ...args);
}

export function logError(err, ...args) {
  if(err instanceof Error && err.message === 'terminated') {
    err = err.message;
  }
  console.error(formatDate(), err, ...args);
}

/**
 * @summary Создаёт массив слушателей изменений массива баз Couchdb
 * @desc Список серверов, извлекает из параметра, записанного в postgres
 */
export class GlobalListener {

  constructor(postgres, branches) {
    this.postgres = postgres;
    this.branches = branches;
    this.abonents = new Map();
    setInterval(this.stat.bind(this), statInterval);
  }

  async listen() {
    const {postgres, branches, abonents} = this;
    const items = await postgres.get('listen');
    for(const {year, abonent, skip} of items) {
      if(!skip) {
        if(!abonents.has(abonent)) {
          abonents.set(abonent, {});
        }
        const servers = abonents.get(abonent);
        const rows = await postgres.servers({year, abonent});
        for(const {addr} of rows) {
          if(!servers[addr]) {
            servers[addr] = new ServerListener({year, abonent, postgres, branches, addr});
          }
        }
      }
    }
    for(const [abonent, servers] of abonents) {
      for(const addr in servers) {
        await servers[addr].listen();
      }
    }
  }

  stopAll() {
    for(const [abonent, servers] of this.abonents) {
      for(const addr in servers) {
        servers[addr].stopAll();
      }
    }
  }

  async stat() {
    const aggregate = {dbs: 0, active: 0, current: 0, all: 0, speed: 0, errors: 0};
    for(const [abonent, servers] of this.abonents) {
      for(const addr in servers) {
        const {moment, ...other} = await servers[addr].stat();
        for(const fld in other) {
          aggregate[fld] += other[fld];
        }
      }
    }
    aggregate.moment = formatDate();
    await this.postgres.set(`stat:aggregate`, aggregate);
    return aggregate;
  }
}

/**
 * @summary Создаёт слушателей изменений массива баз Couchdb
 * @desc На конкретном сервере
 */
class ServerListener {

  constructor({year, abonent, postgres, branches, addr}) {
    this.year = year;
    this.abonent = abonent;
    this.postgres = postgres;
    this.branches = branches;
    this.url = addr;
    this.root = new Couchdb(`${addr}/_all_dbs`, {
      auth: {username: DBUSER, password: DBPWD},
      skip_setup: true,
    });
    // базы
    this.bases = {};
    // фиды баз
    this.feeds = new Map();
  }

  /**
   * @summary Сохраняет в PG, текущую статистику
   */
  async stat() {
    const {postgres, url, abonent} = this;
    const key = `stat:${url}:${abonent}`;
    const aggregate = {
      dbs: 0,
      active: 0,
      current: 0,
      all: await postgres.get(key)?.all || 0,
      speed: 0,
      errors: 0,
    };
    for(const [db] of this.feeds) {
      const stat = await this.statDb(db);
      aggregate.dbs++;
      if(stat.current) {
        aggregate.active++;
        aggregate.current += stat.current;
        aggregate.all += stat.current;
        if(stat.speed) {
          aggregate.speed += stat.speed;
        }
      }
      if(stat.error) {
        aggregate.errors++;
      }
    }
    aggregate.moment = formatDate();
    await this.postgres.set(key, aggregate);
    return aggregate;
  }

  async statDb(db) {
    const {feeds, postgres} = this;
    const stat = feeds.get(db);
    const newStat = await postgres.stat(db);
    let save = stat.docs;
    if(save) {
      const moment = Math.max(stat.moment, newStat.moment ? reformatDate(newStat.moment) : 0);
      const delta = (new Date() - moment) / 1000;
      newStat.current = save;
      newStat.speed = Math.round(save / delta, 3);
      newStat.all += save;
      delete newStat.error;
      newStat.since = (await postgres.since(db))?.substring(0, 30) || 'nil';
      stat.docs = 0;
    }
    else if(newStat.current) {
      newStat.current = 0;
      save = true;
    }
    if(newStat.error && !stat.feed.isCancelled) {
      delete newStat.error;
      save = true;
    }
    if(save) {
      await postgres.setStat(db, newStat);
    }
    return newStat;
  }

  async statError(db, err) {
    const {feeds, postgres} = this;
    const stat = feeds.get(db);
    const newStat = await postgres.stat(db);
    newStat.current = stat.initStat.current + stat.docs;
    newStat.all = stat.initStat.all + stat.docs;
    newStat.error = err.message || err;
    await postgres.setStat(db, newStat);
  }

  async listen() {
    const {root, url, bases, year, branches, abonent} = this;
    const dbs = branches.sort(await root.info(), abonent, bases);
    for(const {name, branch} of dbs) {
      const db = new Couchdb(`${url}/${name}`, {
        auth: {username: DBUSER, password: DBPWD},
        skip_setup: true,
      });
      db.def = {year, abonent, branch};
      bases[name] = db;
    }
    for(const name in bases) {
      await this.listenDb(bases[name]);
    }
  }

  async listenDb(db) {
    const {feeds, postgres} = this;
    if(!feeds.has(db) || feeds.get(db).feed?.isCancelled) {
      const since = await postgres.since(db);
      const initInfo = await db.info();
      const initStat = await postgres.stat(db);
      initStat.current = 0;
      Object.freeze(initStat);
      Object.freeze(initInfo);
      log(`listen ${db.name.split('//')[1]} since ${since?.substring(0, 30) || 'nil'}`);

      return new Promise((resolve, reject) => {
        const delta = initInfo.doc_count - initStat.all;
        let timeout = (since ? 100 : interval) + (delta > 0 ? delta : 0);
        if(timeout > 15 * 60000) {
          timeout = 15 * 60000;
        }
        const resolveTimer = setTimeout(resolve, timeout);

        const changes = db.changes({
          since,
          live: true,
          include_docs: true,
          return_docs: false,
          style: 'all_docs',
        })
          .on('change', (change) => this.reflect(db, change))
          .on('allReaded', () => {
            clearTimeout(resolveTimer);
            resolve();
          })
          .on('error', async (err) => {
            logError(err, db.name);
            await this.statError(db, err);
            this.stopDb(db);
            clearTimeout(resolveTimer);
            setTimeout(this.listenDb.bind(this, db), db.multiplier * interval);
            if(db.multiplier < 20) {
              db.multiplier *= 2;
            }
            reject();
          });
        const feed = feeds.get(db) || {};
        feeds.set(db, Object.assign(feed, {feed: changes, docs: 0, initStat, initInfo, moment: new Date()}));
      });
    }
  }

  async fetchDate(db, doc, ref) {
    let {date} = doc;
    if(date) {
      dateCache[ref] = date;
    }
    else if(doc.calc_order) {
      date = dateCache[doc.calc_order];
      if(!date) {
        try {
          const raw = await db.get(`doc.calc_order|${doc.calc_order}`);
          date = raw.date;
          dateCache[doc.calc_order] = date;
        }
        catch (e) {
          if(e.status != 404) {
            logError(e);
          }
        }
      }
    }
    return date;
  }

  async reflect(db, change) {
    const {feeds, postgres} = this;
    let {id, changes, seq, doc, deleted} = change;
    const rev = changes?.[0]?.rev || doc._rev;
    const {year, abonent, branch} = db.def;
    if(classNames.test(id)) {
      const [type, ref] = id.split('|');
      let {partner, department} = doc;
      const date = await this.fetchDate(db, doc, ref);
      if(!deleted) {
        deleted = false;
      }
      if(!partner) {
        partner = nil;
      }
      if(!department) {
        department = nil;
      }
      try {
        await postgres.append({year, abonent, branch, type, ref, rev, deleted, partner, department, date});

        const feed = feeds.get(db);
        if(feed) {
          feed.docs++;
          if(feed.docs % 600 === 0) {
            log(`reg ${db.name.split('//')[1]} ${feed.docs} changes`);
          }
        }
      }
      catch (err) {
        const message = err.message || err;
        logError(db.name, id, message);
        // остановка на всех ошибках, кроме формата uuid
        if(!message.includes('uuid: "')) {
          this.stopDb(db);
          throw err;
        }
      }
    }
    db.multiplier = 1;
    await postgres.setSince(db, seq);
  }

  stopDb(db) {
    const feed = this.feeds.get(db);
    if(feed) {
      feed.feed?.cancel?.();
    }
  }

  stopAll() {
    const {feeds} = this;
    for(const [db] of feeds) {
      this.stopDb(db);
    }
    feeds.clear();
  }

}
