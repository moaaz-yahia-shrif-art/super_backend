class RateLimiter {
  constructor(globalPerMinute = 0, perKeyPerMinute = 0) {
    this.rateLimits = {
      globalPerMinute,
      perKeyPerMinute,
    };
    this.rateHistory = {
      global: [],
      perKey: new Map(),
    };
  }

  record(key) {
    const now = Date.now();
    this.rateHistory.global.push(now);
    this.rateHistory.global = this.rateHistory.global.filter(
      (t) => now - t < 60000,
    );

    if (key) {
      if (!this.rateHistory.perKey.has(key))
        this.rateHistory.perKey.set(key, []);
      const arr = this.rateHistory.perKey.get(key);
      arr.push(now);
      const filtered = arr.filter((t) => now - t < 60000);
      if (filtered.length === 0) {
        this.rateHistory.perKey.delete(key);
      } else {
        this.rateHistory.perKey.set(key, filtered);
      }
    }
  }

  canPass(key) {
    const now = Date.now();
    this.rateHistory.global = this.rateHistory.global.filter(
      (t) => now - t < 60000,
    );

    if (
      this.rateLimits.globalPerMinute > 0 &&
      this.rateHistory.global.length >= this.rateLimits.globalPerMinute
    ) {
      return false;
    }

    if (key && this.rateLimits.perKeyPerMinute > 0) {
      const arr = this.rateHistory.perKey.get(key) || [];
      const filtered = arr.filter((t) => now - t < 60000);
      if (filtered.length === 0) {
        this.rateHistory.perKey.delete(key);
      } else if (filtered.length >= this.rateLimits.perKeyPerMinute) {
        return false;
      }
    }
    return true;
  }
}

module.exports = RateLimiter;
