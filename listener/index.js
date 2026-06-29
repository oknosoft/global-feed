
import {Postgres} from './postgres.js';
import {GlobalListener, log} from './listener.js';
import {branchesOrder} from './branches.js';

// запускаем слушатель
setTimeout(async () => {

  const postgres = new Postgres();
  const branches = await branchesOrder();
  const listener = new GlobalListener(postgres, branches);
  listener.listen();

  // планируем перезапуск
  const reloadAt = new Date();
  reloadAt.setDate(reloadAt.getDate() + 1);
  reloadAt.setHours(5, 0, 0);
  //reloadAt.setHours(reloadAt.getHours() + 1, 0, 0);
  setTimeout(() => {
    if(new Date() >= reloadAt) {
      listener.stopAll();
      log('daily restart');
      process.exit(0);
    }
  }, 1200000);
}, 4000);

