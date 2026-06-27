
import PouchDB from 'pouchdb-core';
import adapterHttp from 'pouchdb-adapter-http';
import pouchdbFind from 'pouchdb-find';

PouchDB
  .plugin(adapterHttp)
  .plugin(pouchdbFind);

const {DBUSER, DBPWD} = process.env;
const timeout = 120000;
const interval = 8000;
const docCache = {};
const classNames = /^(doc\.calc_order|cat\.characteristics)/;
const nil = '00000000-0000-0000-0000-000000000000';

function log(...args) {
  console.log(new Date().toISOString().substring(5, 19), ...args);
}

function error(...args) {
  console.error(new Date().toISOString().substring(5, 19), ...args);
}

/**
 * @summary Создаёт массив слушателей изменений массива баз Couchdb
 * @desc Список серверов, извлекает из параметра, записанного в postgres
 */
export class GlobalListener {

  constructor(postgres) {
    this.postgres = postgres;
    this.servers = {};
  }

  /**
   * @summary Возвращает информацию о текущем состоянии слушателей
   */
  async info() {

  }

  async listen() {
    const {postgres, servers} = this;
    const items = await postgres.get('listen');
    for(const {year, abonent, skip} of items) {
      if(!skip) {
        const rows = await postgres.servers({year, abonent});
        for(const {addr} of rows) {
          if(!servers[addr]) {
            servers[addr] = new ServerListener({year, abonent, postgres, addr});
          }
        }
      }
    }
    for(const addr in servers) {
      await servers[addr].listen();
    }
  }
}

/**
 * @summary Создаёт слушателей изменений массива баз Couchdb
 * @desc На конкретном сервере
 */
class ServerListener {

  constructor({year, postgres, addr}) {
    this.postgres = postgres;
    this.url = addr;
    this.year = year;
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

  }

  async listen() {
    const {root, url, bases, year} = this;
    const dbs = await root.info();
    for(const name of dbs) {
      if(/^wb_.*doc/.test(name) && !bases[name]) {
         const db = new PouchDB(`${url}/${name}`, {
          auth: {username: DBUSER, password: DBPWD},
          skip_setup: true,
          ajax: {timeout},
        });
        const parts = name.split('_');
        db.def = {year, abonent: parseInt(parts[1]), branch: parts[3] ? parseInt(parts[3]) : 0};
        bases[name] = db;
      }
    }
    for(const name in bases) {
      await this.listenDb(bases[name]);
    }
    setInterval(() => this.stat(), 300000);
  }

  async listenDb(db) {
    const {feeds, postgres} = this;
    let res;
    if(!feeds.has(db)) {
      const since = await postgres.since(db);
      const info = await db.info();
      const initStat = await postgres.stat(db);
      initStat.current = 0;
      log(`listen ${db.name.split('//')[1]} since ${since?.substring(0, 30) || 'nil'}`);
      res = new Promise((resolve, reject) => {
        let resolved = false;
        const changes = db.changes({
          since,
          live: true,
          include_docs: true,
          style: 'all_docs',
        })
          .on('change', async (change) => {
            await this.reflect(db, change);
            if(!since && !resolved) {
              resolved = true;
              setTimeout(resolve, interval + info.doc_count);
            }
            const feed = feeds.get(db);
            feed.docs++;
            if(feed.docs % 100 === 0) {
              const newStat = {...initStat};
              newStat.current = feed.docs;
              newStat.doc_count += feed.docs;
              await postgres.setStat(db, newStat);
            }
            if(feed.docs % 500 === 0) {
              log(`reg ${db.name.split('//')[1]} ${feed.docs} changes`);
            }
          })
          .on('error', (err) => {
            error(err);
            changes.cancel?.();
            feeds.delete(db);
            setTimeout(() => this.listenDb(db), interval);
            reject();
          });
        feeds.set(db, {feed: changes, docs: 0});
        if(since) {
          setTimeout(resolve, 100 + info.doc_count - initStat.doc_count);
        }
      });
    }
    return res;
  }

  async fetchDate(db, doc, ref) {
    let {date} = doc;
    if(date) {
      docCache[ref] = date;
    }
    else if(doc.calc_order) {
      date = docCache[doc.calc_order];
      if(!date) {
        try {
          const raw = await db.get(`doc.calc_order|${doc.calc_order}`);
          date = raw.date;
          docCache[doc.calc_order] = date;
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

  async stopAll() {

  }

}
