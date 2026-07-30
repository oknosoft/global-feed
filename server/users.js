
import {Couchdb} from '../listener/couchdb.js';
import {branchesOrder} from '../listener/branches.js';
import {authCache} from './usersCache.js';

const user_pass_regexp = /^([^:]*):(.*)$/;
const authPrefix = 'Basic ';
const {DBUSER, DBPWD, COUCHLOCAL, ZONE = 10} = process.env;
const cache = Object.create(null);

class UsersManager {

  #byRef = {};

  constructor(rows, abonents) {
    for(const {doc: {_id, _rev, timestamp, acl, captured, acl_objs, subscribers, ...doc}} of rows) {
      //const abonent = abonents.find(v => v.ref === owner);
      //const branch = {ref, id: parseInt(suffix), suffix, owner: abonent, parent, name, server, children: []};
      if(subscribers) {
        doc.ref = _id.substring(10);
        const abnts = new Set();
        const branches = new Set();
        const branch = abonents.byRef(doc.branch);
        if(branch) {
          branches.add(branch);
        }
        doc.branch = branch;
        for(const row of subscribers) {
          const branch = abonents.byRef(row.branch);
          if(branch) {
            branches.add(branch);
          }
          const abonent = abonents.byRef(row.abonent);
          if(abonent) {
            abnts.add(abonent);
          }
        }
        doc.acl = {
          abonents: Array.from(abnts).map(v => v.id),
          branches: Array.from(branches).map(v => v.id),
        };
        for(const {acl_obj, type} of acl_objs) {
          const area = type.split('.')[1];
          if(area) {
            if(!doc.acl[area]) {
              doc.acl[area] = [];
            }
            if(!doc.acl[area].includes(acl_obj)) {
              doc.acl[area].push(acl_obj);
            }
          }
        }
        if(branch) {
          for(const area of 'organizations,partners,divisions'.split(',')) {
            for(const {acl_obj} of branch[area]) {
              if(!doc.acl[area]) {
                doc.acl[area] = [];
              }
              if(!doc.acl[area].includes(acl_obj)) {
                doc.acl[area].push(acl_obj);
              }
            }
          }
        }

        this.#byRef[doc.ref] = doc;
        this.addToken(doc);
      }
    }
    this.abonents = abonents;
  }

  extractAuth(req) {
    const {authorization} = req.headers;
    if(authorization?.startsWith(authPrefix)) {
      try {
        const key = authorization.substring(authPrefix.length);
        const decoded = user_pass_regexp.exec(Buffer.from(key, 'base64').toString());
        if(decoded) {
          return {key, username: decoded[1], password: decoded[2]};
        }
      }
      catch (e) {

      }
    }
  }

  async queryCouchdb(req) {
    const {authorization} = req.headers;
    return fetch(`${COUCHLOCAL}/_session`, {
      credentials: 'include',
      headers: {Accept: 'application/json', authorization},
    })
      .then(res => res.json())
      .then(res => {
        return res.ok && `org.couchdb.user:${res.userCtx.name}`;
      });
  }

  async authorize(req, res){
    const authorization = this.extractAuth(req);
    if(!authorization) {
      res.statusCode = 401;
      res.setHeader('WWW-Authenticate', 'Basic realm="couchdb auth"');
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.end('Укажите логин и пароль');
      return false;
    }

    let token = authCache.get(authorization.key);
    if(!token) {
      try{
        token = await this.queryCouchdb(req);
      }
      catch (e) {}
      if(!token) {
        const error = new Error(`Неверный логин/пароль '${authorization.username}'`);
        error.status = 401;
        throw error;
      }
      authCache.put(authorization.key, token);
    }

    const user = cache[token];
    if(!user) {
      const error = new Error(`Пользователь '${
        authorization.username}' авторизован, но отсутствует в справочнике 'Пользователи'`);
      error.status = 401;
      throw error;
    }
    const {roles, branch} = user;
    if(roles.includes("doc_full") ||
      roles.includes("_admin") ||
      user.branch && (roles.includes("doc_reader") || roles.includes("doc_editor"))) {
      req.user = user;
      return user;
    }
    const error = new Error(`У пользователя '${authorization.username}', недостаточно прав для доступа к сервису`);
    error.status = 401;
    throw error;
  }

  addToken(user) {
    if(user.ids?.length) {
      for(const {identifier} of user.ids) {
        cache[identifier] = user;
      }
    }
    else if(!user.ancillary && !user.invalid && user.roles?.includes('doc_editor')) {
      cache[`org.couchdb.user:${user.id}`] = user;
    }
    cache[user.ref] = user;
  }

}

export async function loadUsers() {
  const db = new Couchdb(COUCHLOCAL, {auth: {username: DBUSER, password: DBPWD}});
  const res = await db.fetch(`/wb_${ZONE}_ram/_all_docs?start_key="cat.users|"&end_key="cat.users|z"&include_docs=true`);
  const abonents = await branchesOrder();
  return new UsersManager(res.rows, abonents);
}
