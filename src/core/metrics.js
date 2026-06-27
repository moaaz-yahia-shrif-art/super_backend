class Metrics {
  constructor() {
    this.reset();
  }

  reset() {
    this.totalProcessed = 0;
    this.successful = 0;
    this.failed = 0;
    this.totalWaitTime = 0;
    this.totalExecTime = 0;
    this.circuitBreakerTrips = 0;
  }

  recordWaitTime(ms) {
    this.totalWaitTime += ms;
  }

  recordExecTime(ms) {
    this.totalExecTime += ms;
  }

  recordSuccess() {
    this.successful++;
    this.totalProcessed++;
  }

  recordFailure() {
    this.failed++;
    this.totalProcessed++;
  }

  recordCircuitTrip() {
    this.circuitBreakerTrips++;
  }

  snapshot(queueLength, activeCount, circuitState, nodeId) {
    const avgSuccessTime = this.successful
      ? (this.totalExecTime / this.successful).toFixed(1)
      : 0;
    const avgQueueTime = this.totalProcessed
      ? (this.totalWaitTime / this.totalProcessed).toFixed(1)
      : 0;
    return {
      totalProcessed: this.totalProcessed,
      successful: this.successful,
      failed: this.failed,
      totalWaitTime: this.totalWaitTime,
      totalExecTime: this.totalExecTime,
      circuitBreakerTrips: this.circuitBreakerTrips,
      averageExecutionTimeMs: `${avgSuccessTime}ms`,
      averageQueueWaitTimeMs: `${avgQueueTime}ms`,
      currentQueueLength: queueLength,
      activeConcurrency: activeCount,
      circuitState,
      heapUsedMegabytes: (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(
        2,
      ),
      nodeId,
    };
  }
}

module.exports = Metrics;
