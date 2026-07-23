
import querystring from 'node:querystring';

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
    const seq = await this.postgres.query('SELECT seq FROM feed ORDER BY seq desc limit 1');
    info.update_seq = seq.rows[0].seq;
    res.writeHead(200, {...contentType, 'X-Duration': res.took()});
    res.end(JSON.stringify(info));
  }

  async up(res) {
    res.writeHead(200, {...contentType, 'X-Duration': res.took()});
    res.end(JSON.stringify({status: 'ok'}));
  }

  async root(res) {
    res.writeHead(200, {...contentType, 'X-Duration': res.took()});
    res.end(JSON.stringify({
      couchdb: 'Welcome',
      uuid,
      vendor: {
        name: 'The Apache Software Foundation',
      },
      version: "3.5.2",
    }));
  }

  handler(req, res) {
    const {url, method} = req;
    if(url.startsWith('/feed/_changes')) {
      return this.changes(req, res);
    }
    else if(url === '/' || url.startsWith('/?')) {
      return this.root(res);
    }
    else if(url.startsWith('/_up')) {
      return this.up(res);
    }
    else if(url.startsWith('/_') || url.startsWith('/feed/_')) {
      err404(url, `path ${url} not allowed`);
    }
    else if(url === '/feed' || url.startsWith('/feed?') || url.startsWith('/feed/')) {
      if(['GET', 'HEAD'].includes(method)) {
        return this.get(req, res)
      }
      throw Object.assign(new Error(`method ${method} not allowed`), {error: true, status: 405});
    }
    err404(url, `path ${url} not allowed`);
  }
}

