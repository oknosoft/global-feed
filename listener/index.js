
import {Postgres} from './postgres.js';
import {GlobalListener} from './listener.js';

setTimeout(() => {
  const postgres = new Postgres();
  const listener = new GlobalListener(postgres);
  listener.listen();
}, 10000);
