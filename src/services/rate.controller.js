class TokenBucket {
  constructor(capacity, tokensPerInterval, interval) {
    this.capacity = capacity;
    this.tokensPerInterval = tokensPerInterval;
    this.interval = interval;
    this.tokens = capacity;
    this.lastRefill = Date.now();
  }

  refill() {
    const now = Date.now();
    const elapsedTime = now - this.lastRefill;
    if (elapsedTime > this.interval) {
      const tokensToAdd =
        Math.floor(elapsedTime / this.interval) * this.tokensPerInterval;
      this.tokens = Math.min(this.capacity, this.tokens + tokensToAdd);
      this.lastRefill = now;
    }
  }

  async take() {
    this.refill();
    if (this.tokens > 0) {
      this.tokens--;
      return true;
    }
    return false;
  }
}

const buckets = {};

const rateController = (source, capacity, tokensPerInterval, interval) => {
  if (!buckets[source]) {
    buckets[source] = new TokenBucket(capacity, tokensPerInterval, interval);
  }
  return buckets[source];
};

export default rateController;
