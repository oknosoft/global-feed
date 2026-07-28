
import querystring from 'node:querystring';
import {LongPoller} from './longpoll.js';

export const contentType = {'Content-Type': 'application/json; charset=utf-8'};
const classNames = ['cat.characteristics', 'doc.calc_order'];
const uuid = '019f8e43-7b66-7169-968c-6684112c5491';
const seqs = {
  "2018": {
    "8": "019f0fc8-5a85-7221-bf56-c6956d5b99e2",
    "21": "019f0f73-badc-705e-9aa2-6f22ae2150ae",
    "22": "019f1d2c-bedf-73af-a8ec-d73eed18c302"
  },
  "2019": {
    "8": "019f0fc8-5a85-7221-bf56-c6956d5b99e2",
    "21": "019f0fc8-3f99-73e1-a904-7a0d3871faec",
    "22": "019f1d2c-bedf-73af-a8ec-d73eed18c302"
  },
  "2020": {
    "8": "019f193b-5b8f-7836-b05e-2250cdbc5990",
    "21": "019f1294-8fdc-7289-911b-c82edcc2535d",
    "22": "019f1d2c-bedf-73af-a8ec-d73eed18c302"
  },
  "2021": {
    "8": "019f193b-5b8f-7836-b05e-2250cdbc5990",
    "21": "019f17d5-d00d-75bd-bb0f-b9585962e3da",
    "22": "019f1d2c-bedf-73af-a8ec-d73eed18c302"
  },
  "2022": {
    "8": "019f1d2c-f404-75f5-a96e-6b25025dbae0",
    "21": "019f1c61-dd1b-7f20-9d9c-c12b2a1957b5",
    "22": "019f1d2c-bedf-73af-a8ec-d73eed18c302"
  },
  "2023": {
    "8": "019f240f-2530-7273-903c-31e795fb02f3",
    "21": "019f2426-168f-7526-8bd3-d42cb5694013",
    "22": "019f2447-dca5-7381-8024-9a77a767ea61"
  },
  "2024": {
    "8": "019f271e-dd41-73bc-85a9-23304d46da00",
    "21": "019f2733-76be-784a-91fe-d9ae710f7b7b",
    "22": "019f2718-0b08-7041-89d5-6d86ddd23be4"
  },
  "2025": {
    "8": "019f2bb2-fb5c-7b66-bc7f-2c948f31f805",
    "21": "019f2bcf-6932-7291-b26a-38f7cb000d9e",
    "22": "019f2c73-334d-7a51-b40f-4c06c0dd8452"
  },
  "2026": {
    "8": "019f7e89-c86b-7cdf-b217-db0e638e08c1",
    "21": "019f7e97-85ba-7521-bdea-09da3e23f79c",
    "22": "019f80d3-413f-764f-97ba-5cf3880d82c6"
  }
};


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

function getBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => data += chunk);
    req.on('end', () => {
      if(data.length > 0 && data.charCodeAt(0) == 65279) {
        data = data.substring(1);
      }
      resolve(JSON.parse(data));
    });
    req.on('error', reject);
  });
}

export class CouchdbImitator {

  constructor(postgres, users) {
    this.postgres = postgres;
    this.users = users;

    // текущие ожидатели long pool-а
    this.listeners = new Set();

    postgres.get('seqs').then(seqs => {
      this.seqs = seqs;
    })
  }

  async getDoc({type, ref, rev}) {

  }

  async get(req, res) {
    const {url, method, headers} = req;
    let {pathname, rev, attachments, ...params} = parse(url);
    if(!pathname) {
      return this.info(res);
    }
    if(!rev) {
      rev = headers.ETag;
    }
    if(attachments === 'true') {
      attachments = true;
    }
    else {
      attachments = false;
    }
    let [type, ref] = pathname.split('|');
    const isAttachment = ref.includes('/');
    let att = '';
    if(isAttachment) {
      const index = ref.indexOf('/');
      att = ref.substring(index);
      ref = ref.substring(0, index);
    }
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
          if(isAttachment) {
            await servers.direct.attachment(`${type}|${ref}${att}`, rev, res);
            return ;
          }
          else {
            servers.doc = await servers.direct.get(`${type}|${ref}`, rev);
          }
        }
        catch (e) {
          if(servers.direct !== servers.root) {
            if(isAttachment) {
              await servers.root.attachment(`${type}|${ref}${att}`, rev, res);
              return ;
            }
            else {
              servers.doc = await servers.root.get(`${type}|${ref}`, rev, attachments);
            }
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

  async changesBody({limit, include_docs, attachments, selector}) {
    let sql = `SELECT * FROM feed where year = ${selector.year}\n`;
    const attr = [];

    if(selector.abonent) {
      attr.push(selector.abonent);
      if(Array.isArray(selector.abonent)) {
        sql += `and abonent = ANY ($${attr.length})\n`;
      }
      else {
        sql += `and abonent = $${attr.length}\n`;
      }
    }

    if(selector.branch) {
      attr.push(selector.branch);
      if(Array.isArray(selector.branch)) {
        sql += `and branch = ANY ($${attr.length})\n`;
      }
      else {
        sql += `and branch = $${attr.length}\n`;
      }
    }

    if(selector.since) {
      attr.push(selector.since);
      sql += `and seq > $${attr.length}\n`;
    }

    attr.push(limit);
    sql += `ORDER BY seq LIMIT $${attr.length}`;

    const {rows} = await this.postgres.query(sql, attr);

    const last = rows[rows.length - 1];
    const body = {last_seq: last?.seq || since, pending: rows.length === limit ? 1e5 : 0};

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

    body.results = rows.map(({rev, type, ref, seq, deleted, year, abonent, branch, doc}) => {
      const change = {
        changes: [{rev}],
        id: `${type}|${ref}`,
        seq,
        origin: {year, abonent, branch},
      };
      if(deleted) {
        change.deleted = true;
      }
      if(doc) {
        change.doc = doc;
      }
      return change;
    });

    return body;
  }

  /**
   * @summary Компонует селектор из тела запроса и прав текущего пользователя
   * @param req
   * @param since
   * @param filter
   * @return {Promise<void>}
   */
  async changesSelector({req, since, filter}) {
    const {selector} = filter === 'selector' ? await getBody(req) : {selector: {}};
    const {user} = req;

    const err = new Error('invalid selector');
    err.status = 400;

    if(!selector) {
      err.reason = 'empty selector';
      throw err;
    }

    if(!selector.year) {
      selector.year = new Date().getFullYear();
    }
    else if(typeof selector.year !== 'number') {
      err.reason = 'year field must be a number';
      throw err;
    }
    if(selector.year < 2018) {
      selector.year = 2018;
    }


    if(selector.abonent) {
      if(typeof selector.abonent !== 'number') {
        err.reason = 'abonent field must be a number';
        throw err;
      }
    }

    if(selector.branch) {
      if(typeof selector.branch !== 'number') {
        err.reason = 'branch field must be a number';
        throw err;
      }
      if(!selector.abonent) {
        err.reason = 'abonent field must be defined for branch';
        throw err;
      }
    }

    if(!since) {
      if(!selector.abonent) {
        err.reason = 'abonent field must be defined for empty since';
        throw err;
      }
      const seqs = this.seqs[selector.year]?.[selector.abonent];
      if(seqs) {
        since = seqs;
      }
      else {
        err.reason = `since not found for year=${selector.year} and abonent=${selector.abonent}`;
        throw err;
      }
    }
    selector.since = since;

    return selector;
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
    if(heartbeat < 10000) {
      heartbeat = 10000;
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

    const selector = await this.changesSelector({req, since, filter});
    const body = await this.changesBody({limit, include_docs, attachments, selector});

    if (!body.results.length && feed === 'longpoll') {
      res.writeHead(200, contentType);
      this.listeners.add(
        new LongPoller({owner: this, res, since, limit, include_docs, attachments, selector, heartbeat})
      );
    }
    else {
      res.writeHead(200, {...contentType, 'X-Duration': res.took()});
      res.end(JSON.stringify(body));
    }
  }

  async changed(res) {
    res.end();
    for(const listener of this.listeners) {
      await listener.changed();
    }
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

