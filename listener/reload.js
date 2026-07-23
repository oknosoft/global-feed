import {formatDate} from './postgres.js';

export function reload(hour, min, log, listener) {

  const reloadAt = new Date();
  reloadAt.setDate(reloadAt.getDate() + 1);
  reloadAt.setHours(hour, min, 0);
  log(`reload planed at ${formatDate(new Date(reloadAt))}`);
  setInterval(() => {
    if(new Date() >= reloadAt) {
      log('daily restart');
      listener?.stopAll();
      process.exit(0);
    }
  }, 1200000);
}
