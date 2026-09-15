import { Redis } from "@upstash/redis";

// In-memory fallback for local dev when Upstash credentials are not yet configured
class InMemoryRedisFallback {
  private store: Map<string, { value: any; expiresAt?: number }> = new Map();
  private sortedSets: Map<string, Array<{ score: number; member: string }>> = new Map();

  async get<T = any>(key: string): Promise<T | null> {
    const item = this.store.get(key);
    if (!item) return null;
    if (item.expiresAt && Date.now() > item.expiresAt) {
      this.store.delete(key);
      return null;
    }
    return item.value as T;
  }

  async set(key: string, value: any, opts?: { ex?: number }): Promise<string> {
    const expiresAt = opts?.ex ? Date.now() + opts.ex * 1000 : undefined;
    this.store.set(key, { value, expiresAt });
    return "OK";
  }

  async incrby(key: string, amount: number): Promise<number> {
    const current = (await this.get<number>(key)) || 0;
    const next = Number(current) + amount;
    await this.set(key, next);
    return next;
  }

  async zadd(key: string, item: { score: number; member: string }): Promise<number> {
    let set = this.sortedSets.get(key);
    if (!set) {
      set = [];
      this.sortedSets.set(key, set);
    }
    set = set.filter(x => x.member !== item.member);
    set.push(item);
    set.sort((a, b) => a.score - b.score);
    this.sortedSets.set(key, set);
    return 1;
  }

  async zrange(key: string, start: number, stop: number, opts?: { withScores?: boolean }): Promise<any[]> {
    const set = this.sortedSets.get(key) || [];
    const len = set.length;
    const actualStart = start < 0 ? Math.max(0, len + start) : Math.min(start, len);
    const actualStop = stop < 0 ? Math.max(0, len + stop + 1) : Math.min(stop + 1, len);
    const slice = set.slice(actualStart, actualStop);
    return slice.map(x => x.member);
  }

  async zrangebyscore(key: string, min: number | string, max: number | string): Promise<string[]> {
    const set = this.sortedSets.get(key) || [];
    const minVal = min === "-inf" ? -Infinity : Number(min);
    const maxVal = max === "+inf" ? Infinity : Number(max);
    return set.filter(x => x.score >= minVal && x.score <= maxVal).map(x => x.member);
  }

  async zremrangebyscore(key: string, min: number | string, max: number | string): Promise<number> {
    const set = this.sortedSets.get(key) || [];
    const minVal = min === "-inf" ? -Infinity : Number(min);
    const maxVal = max === "+inf" ? Infinity : Number(max);
    const remaining = set.filter(x => x.score < minVal || x.score > maxVal);
    const removed = set.length - remaining.length;
    this.sortedSets.set(key, remaining);
    return removed;
  }
}

let redisInstance: {
  get: <T = any>(key: string) => Promise<T | null>;
  set: (key: string, value: any, opts?: { ex?: number }) => Promise<any>;
  incrby: (key: string, amount: number) => Promise<number>;
  zadd: (key: string, item: { score: number; member: string }) => Promise<any>;
  zrange: (key: string, start: number, stop: number, opts?: { withScores?: boolean }) => Promise<any[]>;
  zrangebyscore: (key: string, min: number | string, max: number | string) => Promise<string[]>;
  zremrangebyscore: (key: string, min: number | string, max: number | string) => Promise<number>;
};

const restUrl = process.env.UPSTASH_REDIS_REST_URL;
const restToken = process.env.UPSTASH_REDIS_REST_TOKEN;

if (restUrl && restToken && restUrl.startsWith("http")) {
  const client = new Redis({
    url: restUrl,
    token: restToken,
  });

  redisInstance = {
    get: async <T = any>(key: string) => client.get<T>(key),
    set: async (key: string, value: any, opts?: { ex?: number }) => {
      if (opts?.ex) {
        return client.set(key, value, { ex: opts.ex });
      }
      return client.set(key, value);
    },
    incrby: async (key: string, amount: number) => client.incrby(key, amount),
    zadd: async (key: string, item: { score: number; member: string }) => client.zadd(key, { score: item.score, member: item.member }),
    zrange: async (key: string, start: number, stop: number, opts?: { withScores?: boolean }) => client.zrange(key, start, stop, opts),
    zrangebyscore: async (key: string, min: number | string, max: number | string) => client.zrange(key, min as any, max as any, { byScore: true }),
    zremrangebyscore: async (key: string, min: number | string, max: number | string) => client.zremrangebyscore(key, min as any, max as any),
  };
} else {
  // Use memory fallback for seamless dev experience
  const fallback = new InMemoryRedisFallback();
  redisInstance = fallback;
}

export const redis = redisInstance;
