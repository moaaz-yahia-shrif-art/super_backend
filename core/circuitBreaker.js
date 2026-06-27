class CircuitBreaker {
  constructor(threshold = 5, cooldown = 30000) {
    this.threshold = threshold;
    this.cooldown = cooldown;
    this.state = "CLOSED";
    this.consecutiveFailures = 0;
    this.openTime = 0;
    this.trips = 0;
  }

  canExecute() {
    if (this.state === "OPEN") {
      if (Date.now() - this.openTime > this.cooldown) {
        this.state = "HALF-OPEN";
        return true;
      }
      return false;
    }
    return true;
  }

  recordSuccess() {
    this.consecutiveFailures = 0;
    if (this.state === "HALF-OPEN") {
      this.state = "CLOSED";
    }
  }

  recordFailure() {
    this.consecutiveFailures++;
    if (this.consecutiveFailures >= this.threshold && this.state !== "OPEN") {
      this.state = "OPEN";
      this.openTime = Date.now();
      this.trips++;
    }
  }

  getState() {
    return this.state;
  }

  getTrips() {
    return this.trips;
  }
}

module.exports = CircuitBreaker;
