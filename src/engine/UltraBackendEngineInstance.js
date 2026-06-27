const UltraBackendEngine = require("./ultraBackendEngine");

const engine = new UltraBackendEngine({
  concurrency: 2,
  maxConcurrency: 6,
  maxQueueLimit: 500,
  globalPerMinute: 120,
  memoryLimit: 0.9,
});

if (typeof engine.getMetrics !== "function") {
  engine.getMetrics = () => ({
    running: engine.runningTasks || 0,
    queued: engine.queue?.length || 0,
    completed: engine.completedTasks || 0,
    rejected: engine.rejectedTasks || 0,
  });
}

if (typeof engine.getQueueSnapshot !== "function") {
  engine.getQueueSnapshot = () => engine.queue || [];
}

if (typeof engine.getAuditLogs !== "function") {
  engine.getAuditLogs = () => engine.auditLogs || [];
}

module.exports = engine;
