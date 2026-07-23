
import { hrtime } from 'node:process';

const NS_PER_SEC = 1e9;

export function took() {
  const start = hrtime();
  return () => {
    const diff = hrtime(start);
    const duration = Math.round((diff[0] * NS_PER_SEC + diff[1])/1e5)/10;
    return duration;
  }
}
