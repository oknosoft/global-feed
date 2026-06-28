
import PouchDB from 'pouchdb-core';
import adapterHttp from 'pouchdb-adapter-http';
import pouchdbFind from 'pouchdb-find';
import {formatDate} from './postgres.js';

PouchDB
  .plugin(adapterHttp)
  .plugin(pouchdbFind);

const {DBUSER, DBPWD} = process.env;
const timeout = 120000;       // удлиняем время ответа для PouchDB
const interval = 8000;        // задержка между базами на старте и при ошибке
const statInterval = 180000;  // статистика раз в 5 минут
const heartbeat = 25000;      // to keep long connections open
const dateCache = {};             // даты документов
const classNames = /^(doc\.calc_order|cat\.characteristics)/;
const nil = '00000000-0000-0000-0000-000000000000';

export function log(...args) {
  console.log(formatDate(), ...args);
}

function error(...args) {
  console.error(formatDate(), ...args);
}

/**
 * @summary Создаёт массив слушателей изменений массива баз Couchdb
 * @desc Список серверов, извлекает из параметра, записанного в postgres
 */
export class GlobalListener {

  constructor(postgres) {
    this.postgres = postgres;
    this.abonents = new Map();
  }

  async listen() {
    const {postgres, abonents} = this;
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
            servers[addr] = new ServerListener({year, abonent, postgres, addr});
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
}

/**
 * @summary Создаёт слушателей изменений массива баз Couchdb
 * @desc На конкретном сервере
 */
class ServerListener {

  constructor({year, abonent, postgres, addr}) {
    this.year = year;
    this.abonent = abonent;
    this.postgres = postgres;
    this.url = addr;
    this.root = new PouchDB(`${addr}/_all_dbs`, {
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
    for(const [db] of this.feeds) {
      await this.statDb(db);
    }
  }

  async statDb(db) {
    const {feeds, postgres} = this;
    const stat = feeds.get(db);
    const newStat = await postgres.stat(db);
    if(stat.docs > newStat.current) {
      newStat.current = stat.docs;
      newStat.all = stat.initStat.all + stat.docs;
      delete newStat.error;
      await postgres.setStat(db, newStat);
    }
  }

  async statError(db, err) {
    const {feeds, postgres} = this;
    const stat = feeds.get(db);
    const newStat = await postgres.stat(db);
    newStat.current = stat.docs;
    newStat.all = stat.initStat.all + stat.docs;
    newStat.error = err.message || err;
    await postgres.setStat(db, newStat);
  }

  async listen() {
    const {root, url, bases, year, abonent} = this;
    const dbs = await root.info();
    for(const name of dbs) {
      if(name.startsWith(`wb_${abonent}`) && name.includes('_doc') && !bases[name]) {
        const parts = name.split('_');
        if(!parts[3] || /^\d+$/.test(parts[3])) {
          const db = new PouchDB(`${url}/${name}`, {
            auth: {username: DBUSER, password: DBPWD},
            skip_setup: true,
            ajax: {timeout},
          });
          db.def = {year, abonent: parseInt(parts[1]), branch: parts[3] ? parseInt(parts[3]) : 0};
          bases[name] = db;
        }
      }
    }
    setInterval(() => this.stat(), statInterval);
    for(const name in bases) {
      await this.listenDb(bases[name]);
    }
  }

  async listenDb(db) {
    const {feeds, postgres} = this;
    let res;
    if(!feeds.has(db) || !feeds.get(db).feed) {
      const since = await postgres.since(db);
      const initInfo = await db.info();
      const initStat = await postgres.stat(db);
      Object.freeze(initStat);
      Object.freeze(initInfo);
      log(`listen ${db.name.split('//')[1]} since ${since?.substring(0, 30) || 'nil'}`);

      res = new Promise((resolve, reject) => {
        const delta = initInfo.doc_count - initStat.all;
        const timeout = (since ? 100 : interval) + (delta > 0 ? delta : 0);
        const resolver = setTimeout(resolve, timeout);
        const restart = this.listenDb.bind(this, db);
        const changes = db.changes({
          since,
          live: true,
          include_docs: true,
          style: 'all_docs',
          heartbeat,
        })
          .on('change', async (change) => {
            await this.reflect(db, change);
            const feed = feeds.get(db);
            if(feed) {
              feed.docs++;
              if(feed.docs % 500 === 0) {
                log(`reg ${db.name.split('//')[1]} ${feed.docs} changes`);
              }
              if(feed.feed && feed.docs > 2000) {
                !feed.feed.isCancelled && feed.feed.cancel();
                this.statDb(db);
                delete feed.feed;
                resolver && clearTimeout(resolver);
                setTimeout(restart, 100);
                resolve();
              }
            }
          })
          .on('error', (err) => {
            error(err);
            this.statError(db, err);
            const feed = feeds.get(db);
            if(feed?.feed) {
              !feed.feed.isCancelled && feed.feed.cancel();
              delete feed.feed;
            }
            resolver && clearTimeout(resolver);
            setTimeout(restart, interval);
            reject();
          });
        const feed = feeds.get(db) || {};
        feeds.set(db, Object.assign(feed, {feed: changes, docs: 0, initStat, initInfo}));
      });
    }
    return res;
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
            error(e);
          }
        }
      }
    }
    return date;
  }

  async reflect(db, change) {
    const {postgres} = this;
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
      await postgres.append({year, abonent, branch, type, ref, rev, deleted, partner, department, date});
      return postgres.setSince(db, seq);
    }
  }

  stopAll() {
    const {feeds} = this;
    for(const [db, feed] of feeds) {
      feed.feed?.cancel?.();
    }
    feeds.clear();
  }

}
