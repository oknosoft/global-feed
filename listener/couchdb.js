
// import EventEmitter from 'node:events';
import {sleepTimeout} from './postgres.js';

export const nil = '00000000-0000-0000-0000-000000000000';

/**
 * @summary Аналог PouchDB.Changes в live-режиме
 * @desc Слушает базу Couchdb, перезапускает при ошибке.
 * Отличается от оригинала, асинхронным обработчиком событий
 */
class Subscriber {

  constructor(owner, opts) {
    this.owner = owner;
    this.since = opts.since;
    this.allReaded = false;
    this.handlers = {};
    this.fetch = this.fetch.bind(this);
    Promise.resolve().then(this.fetch);
  }

  on(event, handler) {
    this.handlers[event] = handler;
    return this;
  }

  async fetch() {
    if(this.isCancelled) {
      return;
    }
    const {since, owner: {name, headers}, allReaded, handlers} = this;
    let url = `${name}/_changes?heartbeat=40000&style=all_docs&include_docs=true&limit=40`;
    if(allReaded) {
      url += `&feed=longpoll`;
    }
    if(since) {
      url += `&since=${since}`;
    }
    if(!headers.has('Connection')) {
      headers.set('Connection', 'keep-alive');
    }

    try {
      this.controller = new AbortController();
      const { signal } = this.controller;
      const {last_seq, results, pending} = await fetch(url, {headers, signal})
        .then(res => res.json());
      const {change} = handlers;
      for(const item of results) {
        await change(item);
        this.since = item.seq;
      }
      if(!pending && !allReaded) {
        this.allReaded = true;
        handlers.allReaded?.();
      }
      if(!this.isCancelled) {
        setTimeout(this.fetch, sleepTimeout);
      }
    }
    catch (e) {
      if (e.name !== 'AbortError') {
        this.handlers.error(e);
      }
    }
  }

  cancel() {
    this.controller?.abort();
    this.isCancelled = true;
  }
}

/**
 * @summary Аналог базы PouchDB
 * @desc Предоставляет ограниченный и упрощенный интерфейс с нативным fetch()
 */
export class Couchdb {

  constructor(name, {auth}) {
    this.name = name;
    this.headers = new Headers({
      Accept: 'application/json',
      Authorization: `Basic ${Buffer.from(auth.username + ':' + auth.password, 'utf8').toString('base64')}`,
    });
    this.multiplier = 1;
  }

  fetch(path = '', opts) {
    const {name, headers} = this;
    return fetch(name + path, {headers, ...opts})
      .then(res => res.json());
  }

  info() {
    return this.fetch();
  }

  changes(opts) {
    return new Subscriber(this, opts);
  }

  get(id) {
    const {name, headers} = this;
    return fetch(`${name}/${id}`, {headers})
      .then(res => res.json());
  }
}
