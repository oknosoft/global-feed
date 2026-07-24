
export const port = process.env.PORT || 3099;
const delay = 70;

export class Emitter {

  #timer = null;
  #start = 0;
  #emiting = false;

  constructor() {
    this.emit = this.emit.bind(this);
    this.#start = Date.now();
  }

  notify() {
    const duration = Date.now() - this.#start;
    if(duration < delay && this.#timer) {
      clearTimeout(this.#timer);
    }
    this.#timer = setTimeout(this.emit, delay);
  }

  emit() {
    this.#start = Date.now();
    const signal = AbortSignal.timeout(2000);
    fetch(`http://localhost:${port}/changed`, {signal})
      .catch(() => null)
      .then(() => {
        this.#emiting = false;
      });
  }


}
