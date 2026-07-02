
import {Postgres, formatDate} from './postgres.js';
import {GlobalListener, log, logError} from './listener.js';
import {branchesOrder} from './branches.js';

// запускаем слушатель
setTimeout(async () => {

  log(`start`);
  const postgres = new Postgres();
  const branches = await branchesOrder();
  const listener = new GlobalListener(postgres, branches);
  listener.listen();

  // планируем перезапуск по ночам
  const reloadAt = new Date();
  reloadAt.setDate(reloadAt.getDate() + 1);
  reloadAt.setHours(3, 30, 0);
  log(`reload planed at ${formatDate(reloadAt)}`);
  setInterval(() => {
    if(new Date() >= reloadAt) {
      log('daily restart');
      listener.stopAll();
      process.exit(0);
    }
  }, 1200000);

  // лог необработанных ошибок
  process.on('unhandledRejection', (error) => {
    if(error && error.status !== 404) {
      logError(`unhandledRejection`, err.cause?.code === 'ECONNREFUSED' ? `${err.cause.code} ${err.cause.address}` :  error);
    }
  });

}, 4000);

