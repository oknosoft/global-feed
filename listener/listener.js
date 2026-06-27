
import PouchDB from 'pouchdb-core';
import adapterHttp from 'pouchdb-adapter-http';
import pouchdbFind from 'pouchdb-find';

PouchDB
  .plugin(adapterHttp)
  .plugin(pouchdbFind);

const {DBUSER, DBPWD} = process.env;
const timeout = 120000;
const interval = 10000;
const docCache = {};
const classNames = /^(doc\.calc_order|cat\.characteristics)/;

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
   * @summary Возвращает информацию о текущем состоянии слушателей
   */
  async info() {

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
  }

  async listenDb(db) {
    const {feeds, postgres} = this;
    let res;
    if(!feeds.has(db)) {
      const since = await postgres.since(db);
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
              setTimeout(resolve, interval);
            }
          })
          .on('error', (err) => {
            console.error(err);
            changes.stop?.();
            feeds.delete(db);
            setTimeout(() => this.listenDb(db), interval);
            reject();
          });
        feeds.set(db, changes);
        if(since) {
          setTimeout(resolve, 100);
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
            console.error(e);
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
      const {partner, department} = doc;
      const date = await this.fetchDate(db, doc, ref);
      if(!deleted) {
        deleted = false;
      }
      await postgres.append({year, abonent, branch, type, ref, rev, deleted, partner, department, date});
      return postgres.setSince(db, seq);
    }
  }

  async stopAll() {

  }

}
