
import querystring from 'node:querystring';
import {nil} from '../listener/couchdb.js';

export const contentType = {'Content-Type': 'application/json; charset=utf-8'};
const classNames = ['cat.characteristics', 'doc.calc_order'];
const uuid = '019f8e43-7b66-7169-968c-6684112c5491';

function parse(url) {
  if(url) {
    const unescaped = querystring.unescape(url);
    const index = unescaped.indexOf('?');
    const pathname = index < 0 ? unescaped.substring(6) : unescaped.substring(6, index);
    const query = unescaped.substring(index+1);
    if(query) {
      return {...querystring.parse(query), pathname};
    }
  }
  return {pathname: ''};
}

function err404(url, message, reason) {
  const err = new Error(message || `${url} not found`);
  err.status = 404;
  err.error = true;
  if(reason) {
    err.reason = reason;
  }
  throw err;
}

function err405(method, message) {
  const err = new Error(message || `method ${method} not allowed`);
  err.status = 405;
  err.error = true;
  throw err;
}

export class CouchdbImitator {

  constructor(postgres) {
    this.postgres = postgres;
  }

  async getDoc({type, ref, rev}) {

  }

  async get(req, res) {
    const {url, method, headers} = req;
    let {pathname, rev, ...params} = parse(url);
    if(!pathname) {
      return this.info(res);
    }
    if(!rev) {
      rev = headers.ETag;
    }
    const [type, ref] = pathname.split('|');
    // если передали If-None-Match...
    if(!rev && headers['if-none-match']) {
      const row = await this.postgres.lastRev({type, ref, row: true});
      if(headers['if-none-match'].includes(row?.rev)) {
        res.writeHead(304, {ETag: headers['if-none-match']});
        return res.end();
      }
    }
    if(classNames.includes(type)) {
      const row = await this.postgres.docRow({type, ref, rev, strict: true});
      if(row) {
        if(row.deleted) {
          return err404(pathname, null, 'deleted');
        }
        const servers = await this.postgres.servers(row);
        try {
          servers.doc = await servers.direct.get(`${type}|${ref}`, rev);
        }
        catch (e) {
          if(servers.direct !== servers.root) {
            servers.doc = await servers.root.get(`${type}|${ref}`, rev);
          }
        }
        const body = JSON.stringify(servers.doc);
        res.writeHead(200, {...contentType, ETag: `"${servers.doc._rev}"`, 'X-Duration': res.took()});
        return res.end(body);
      }
      if(rev) {
        pathname += `?rev=${rev}`;
      }
    }

    err404(pathname);
  }

  async changes(req, res) {
    const {url, method, headers} = req;
    let {
      pathname,
      filter,
      feed = 'normal',
      doc_ids,
      descending,
      heartbeat = 40000,
      include_docs,
      attachments,
      limit = 40,
      since,
      style, ...other} = parse(url);

    if(filter === 'selector' && method !== 'POST') {
      err405(method, `method GET not allowed for 'filter=selector'`);
    }

    if(descending === 'true') {
      err405(method, `only ASC descending allowed`);
    }

    if(!['normal', 'longpoll'].includes(feed)) {
      err405(method, `only normal and longpoll feeds allowed`);
    }

    if(!since && headers['last-event-id']) {
      since = headers['last-event-id'];
    }
    if(since === 'now') {
      since = await this.postgres.lastSeq();
    }

    if(typeof limit === 'string') {
      limit = parseInt(limit);
    }
    if(limit > 100) {
      limit = 100;
    }

    if(heartbeat === 'true') {
      heartbeat = 40000;
    }
    else if(typeof heartbeat === 'string') {
      heartbeat = parseInt(heartbeat);
    }

    if(include_docs === 'true') {
      include_docs = true;
    }
    else {
      include_docs = false;
    }

    if(attachments === 'true') {
      attachments = true;
      include_docs = true;
    }
    else {
      attachments = false;
    }

    const {rows} = await this.postgres
      .query('SELECT * FROM feed where seq > $1 ORDER BY seq LIMIT $2', [since || nil, limit]);

    const last = rows[rows.length - 1];
    const body = {last_seq: last.seq, pending: rows.length === limit ? 1e5 : 0};

    if(include_docs) {
      // сгруппируем по серверу
      const servers = new Map();
      for(const row of rows) {
        const {direct} = await this.postgres.servers(row);
        if(!servers.has(direct)) {
          servers.set(direct, []);
        }
        servers.get(direct).push(row);
      }
      for(const [server, rows] of servers) {
        const {results} = await server.bulk_get(rows.map(({type, ref, rev}) =>
          ({id: `${type}|${ref}`, rev})));
        rows.forEach((row, ind) => {
          const {ok, error} = results[ind].docs[0];
          row.doc = ok || error;
        });
      }
    }

    body.results = rows.map(({rev, type, ref, seq, deleted, doc}) => {
      const change = {
        changes: [{rev}],
        id: `${type}|${ref}`,
        seq,
      };
      if(deleted) {
        change.deleted = true;
      }
      if(doc) {
        change.doc = doc;
      }
      return change;
    });

    res.writeHead(200, {...contentType, 'X-Duration': res.took()});
    return res.end(JSON.stringify(body));

  }

  async info(res) {
    const info = {
      cluster: {
        q: 1,
        n: 1,
        r: 1,
        w: 1,
      },
      compact_running: false,
      db_name: 'feed',
      instance_start_time: '0',
      purge_seq: '0',
      doc_count: 1e7,
      doc_del_count : 0,
      disk_format_version: 8,
    };
    const seq = await this.postgres.lastSeq();
    res.writeHead(200, {...contentType, 'X-Duration': res.took()});
    res.end(JSON.stringify(info));
  }

  up(res) {
    res.writeHead(200, {...contentType, 'X-Duration': res.took()});
    res.end(JSON.stringify({status: 'ok'}));
  }

  root(res) {
    res.writeHead(200, {...contentType, 'X-Duration': res.took()});
    res.end(JSON.stringify({
      couchdb: 'Welcome',
      uuid,
      vendor: {
        name: 'The Apache Software Foundation',
      },
      version: "3.0.1",
    }));
  }

  all_dbs(res) {
    res.writeHead(200, {...contentType, 'X-Duration': res.took()});
    res.end(`["feed"]`);
  }

  handler(req, res) {
    const {url, method} = req;
    if(url.startsWith('/feed/_changes')) {
      return this.changes(req, res);
    }
    if(url === '/' || url.startsWith('/?')) {
      return this.root(res);
    }
    if(url.startsWith('/_up')) {
      return this.up(res);
    }
    if(url.startsWith('/_all_dbs')) {
      return this.all_dbs(res);
    }
    if(url.startsWith('/_') || url.startsWith('/feed/_')) {
      err404(url, `path ${url} not allowed`);
    }
    else if(url === '/feed' || url.startsWith('/feed?') || url.startsWith('/feed/')) {
      if(['GET', 'HEAD'].includes(method)) {
        return this.get(req, res)
      }
      err405(method);
    }
    err404(url, `path ${url} not allowed`);
  }
}

