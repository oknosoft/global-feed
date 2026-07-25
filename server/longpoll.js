/**
 * @summary Слушает изменения и отвечает на запрос, если возникли подходящие данные
 * @param {ServerResponse} res - node:http.ServerResponse
 * @param {CouchdbImitator} owner - владелец слушателя, имитатор Couchdb
 * @param {String} since
 * @param {Number} limit
 * @param {Boolean} include_docs
 * @param {Boolean} attachments
 * @param {Object} selector
 */
export class LongPoller {

  #attr = null;

  constructor(attr) {
    const {res, heartbeat} = attr;
    this.#attr = attr;
    this.#attr.timer = setInterval(() => res.write('\n'), heartbeat);
  }

  async changed() {
    const {owner, res, since, limit, include_docs, attachments, selector} = this.#attr;
    const body = await owner.changesBody({since, limit, include_docs, attachments, selector});
    if(body.results.length) {
      // отключаем ping
      clearInterval(this.#attr.timer);
      // перестаём слушать события
      owner.listeners.delete(this);
      // отвечаем на запрос
      res.end(JSON.stringify(body));
      // чистим реквизиты не дожидаясь сборщика мусора
      for(const fld in this.#attr) {
        delete this.#attr[fld];
      }
    }
  }

}
