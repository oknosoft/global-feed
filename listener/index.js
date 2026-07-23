
import {Postgres} from './postgres.js';
import {GlobalListener, log, logError} from './listener.js';
import {branchesOrder} from './branches.js';
import {currentServers} from './servers.js';
import {reload} from './reload.js';

// запускаем слушатель
setTimeout(async () => {

  log(`start`);
  const postgres = new Postgres();
  const branches = await branchesOrder();
  await currentServers(postgres, branches);
  const listener = new GlobalListener(postgres, branches);
  listener.listen();

  // планируем перезапуск по ночам
  reload(3, 3, log, listener);

  // лог необработанных ошибок
  process.on('unhandledRejection', (err) => {
    if(err && err.status !== 404) {
      logError(`unhandledRejection`, err.cause?.code === 'ECONNREFUSED' ? `${err.cause.code} ${err.cause.address}` :  err);
    }
  });

}, 3000);

