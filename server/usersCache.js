/**
 * Кеш аутентификации
 *
 */

const liveTime = 400000;
const usersCache = Object.create(null);

function clearCache(force) {
  const now = Date.now();
  const del = [];
  for(const k in usersCache) {
    if(force || (usersCache[k].stamp + liveTime < now)) {
      del.push(k);
    }
  }
  for(const k of del) {
    delete usersCache[k];
  }
  !force && setTimeout(clearCache, liveTime);
}
clearCache();

export const authCache = {
  get(key) {
    const el = usersCache[key];
    return el && el.val;
  },
  ext(key) {
    const el = usersCache[key];
    return el && el.ext;
  },
  put(key, val, ext) {
    usersCache[key] = {val, ext, stamp: Date.now()};
  },
  del(key) {
    delete usersCache[key];
  },
  reset() {
    clearCache(true);
  }
};
