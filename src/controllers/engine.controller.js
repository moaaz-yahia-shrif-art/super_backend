const actionsRegistry = require("../core/actionsRegistry");
const engine = require("../engine/UltraBackendEngineInstance");

exports.executeAction = (req, res) => {
  const { action, data, priority, key, timeout } = req.body;

  const targetFunction = actionsRegistry[action];
  if (!targetFunction) {
    return res.status(400).json({
      error: `The action [${action}] is not registered.`,
    });
  }

  const taskId = engine.addTask(
    () => targetFunction(data),
    { priority: priority || "normal", key, timeout },
    (err, result) => {
      if (res.headersSent) return;
      if (err) return res.status(500).json({ error: err.message });
      return res.json({ status: "success", action, result });
    },
  );

  if (!taskId && !res.headersSent) {
    return res.status(429).json({
      error:
        "Task rejected (Rate limit, Circuit Breaker, or Memory Protection).",
    });
  }
};

exports.dashboard = (req, res) => {
  res.json({
    metrics: engine.getMetrics(),
    queue: engine.getQueueSnapshot(),
    auditLogs: engine.getAuditLogs(),
    nodeId: engine.nodeId,
  });
};
