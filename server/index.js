
import { RateLimiterMemory } from 'rate-limiter-flexible';
import http from 'node:http';
import {sleep} from '../listener/postgres.js';
import {log, logError} from '../listener/listener.js';
import {port} from '../listener/emitter.js';
import {CouchdbImitator, contentType} from './couchdb.js';
import {took} from './stat.js';
import {reload} from '../listener/reload.js';
import {PostgresClient} from './postgres.js';

const opts = {
  points: 10, // разрешаем 10 запросов
  duration: 3, // в течение трёх секунд
};


// запускаем сервер
setTimeout(async () => {

  const postgres = new PostgresClient();
  await postgres.init();
  log(`postgres initiated`);

  const imitator = new CouchdbImitator(postgres);

  const ipLimiter = new RateLimiterMemory(opts);

  const server = http.createServer((req, res) => {
    // проверяем лимит запросов в секунду
    res.took = took();
    const {remoteAddress} = res.socket;
    const {headers} = req;
    let key = headers['x-forwarded-for'] || headers['x-real-ip'] || remoteAddress;
    // запросы listener-а, выполняем без промедления
    if(key === '::1' || key.includes('127.0.0.1')) {
      return imitator.changed(res);
    }
    ipLimiter.consume(key, 1)
      .catch((limiterRes) => {
        if(limiterRes instanceof Error || limiterRes.consumedPoints > 6) {
          const err = new Error('Too many requests');
          res.writeHead(429, {
            "Retry-After": limiterRes.msBeforeNext / 1000,
            "X-RateLimit-Limit": opts.points,
            "X-RateLimit-Remaining": limiterRes.remainingPoints,
            "X-RateLimit-Reset": Math.ceil((Date.now() + limiterRes.msBeforeNext) / 1000)
          });
          res.end(err.message);

          return err;
        }
        return sleep(limiterRes.msBeforeNext).then(() => limiterRes);
      })
      .then((limiter) => limiter instanceof Error ? null : imitator.handler(req, res))
      .catch((err) => {
        const status = err.status || 500;
        res.writeHead(status, {...contentType, 'X-Duration': res.took()});
        const body = {
          error: true,
          status,
          message: err?.message || err,
        };
        if(err.reason) {
          body.reason = err.reason;
        }
        res.end(JSON.stringify(body));
      });
  });
  server.listen(port);
  log(`server listen on port: ${port}`);


  // планируем перезапуск по ночам
  reload(3, 30, log);

  // лог необработанных ошибок
  process.on('unhandledRejection', (err) => {
    if(err && err.status !== 404) {
      logError(`unhandledRejection`, err.cause?.code === 'ECONNREFUSED' ? `${err.cause.code} ${err.cause.address}` :  err);
    }
  });

}, 5000);
