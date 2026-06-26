class UltraBackendEngine {
  constructor(options = {}) {
    this.queue = [];
    this.taskKeys = new Set();
    this.auditLogs = [];
    this.activeCount = 0;
    this.isPaused = false;
    this.consecutiveFailures = 0;
    this.isCircuitOpen = false;
    this.isShuttingDown = false;
    this.taskIdCounter = 0;
    this.enableLogging =
      options.enableLogging !== undefined ? options.enableLogging : true;
    this.nodeId =
      options.nodeId || `node_${Math.random().toString(36).substr(2, 5)}`;
    this.distributeStrategy = options.distributeStrategy || "local";
    this.storageAdapter = options.storageAdapter || null;
    this.plugins = [];
    this.workflows = new Map();
    this.completedTasks = new Set();
    this.activeTasks = new Map();
    this.traces = [];
    this.rateLimits = {
      globalPerMinute: options.globalPerMinute || 0,
      perKeyPerMinute: options.perKeyPerMinute || 0,
    };
    this.rateHistory = {
      global: [],
      perKey: new Map(),
    };

    this.baseConcurrency = options.concurrency || 2;
    this.maxConcurrency = options.maxConcurrency || 6;
    this.maxQueueLimit = options.maxQueueLimit || 1500;
    this.maxLogLimit = options.maxLogLimit || 500;
    this.defaultTimeout = options.defaultTimeout || 10000;
    this.maxRetries = options.maxRetries || 2;
    this.circuitThreshold = options.circuitBreakerThreshold || 5;
    this.memoryLimit =
      options.memoryLimit !== undefined ? options.memoryLimit : 0.96;

    this.metrics = {
      totalProcessed: 0,
      successful: 0,
      failed: 0,
      totalWaitTime: 0,
      totalExecTime: 0,
    };
    this.listeners = {
      onTaskStart: [],
      onTaskSuccess: [],
      onTaskFailed: [],
      onQueueFull: [],
      onDrain: [],
      onShutdown: [],
    };
  }

  async initFromStorage() {
    if (!this.storageAdapter || !this.storageAdapter.loadPendingTasks) return;
    const tasks = await this.storageAdapter.loadPendingTasks(this.nodeId);
    for (const t of tasks) {
      this.queue.push(t);
      this.taskKeys.add(t.key);
    }
  }

  registerPlugin(plugin) {
    this.plugins.push(plugin);
  }

  applyPlugins(event, payload) {
    for (const plugin of this.plugins) {
      if (typeof plugin[event] === "function") {
        plugin[event](payload, this);
      }
    }
  }

  defineWorkflow(name, steps) {
    this.workflows.set(name, steps);
  }

  addToAuditLog(taskId, status, details = "") {
    if (this.auditLogs.length >= this.maxLogLimit) this.auditLogs.shift();
    if (this.traces.length >= this.maxLogLimit) this.traces.shift();

    const entry = {
      taskId,
      status,
      details,
      timestamp: new Date().toISOString(),
      nodeId: this.nodeId,
    };
    this.auditLogs.push(entry);
    this.traces.push({ type: "audit", entry });
  }

  getAuditLogs() {
    return this.auditLogs;
  }

  clearAuditLogs() {
    this.auditLogs = [];
    this.traces = [];
    this.log("info", "Audit logs cleared.");
  }

  generateTaskId() {
    this.taskIdCounter += 1;
    return `t_${this.nodeId}_${this.taskIdCounter}`;
  }

  recordRate(key) {
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

  canPassRateLimit(key) {
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

  addTask(taskFn, options = {}, callback) {
    if (
      this.isCircuitOpen ||
      this.isShuttingDown ||
      process.memoryUsage().heapUsed / process.memoryUsage().heapTotal >
        this.memoryLimit
    ) {
      if (callback)
        callback(
          new Error("System Protection Mode Active: Task Rejected."),
          null,
        );
      return null;
    }

    if (!this.canPassRateLimit(options.key || null)) {
      if (callback) callback(new Error("Rate limit exceeded."), null);
      return null;
    }

    if (options.key && this.taskKeys.has(options.key)) {
      this.log("warn", `Task blocked. Duplicate key found: ${options.key}`);
      return null;
    }

    if (this.queue.length > this.maxQueueLimit * 0.8) {
      this.log("warn", "Backpressure triggered: Slowing down task acceptance.");
      options.ttl = (options.ttl || 30000) - 2000;
    }

    if (this.queue.length >= this.maxQueueLimit) {
      this.emit("onQueueFull", { currentLength: this.queue.length });
      if (callback) callback(new Error("Queue Limit Exceeded."), null);
      return null;
    }

    const basePriority =
      options.priority === "high" ? 1 : options.priority === "low" ? 3 : 2;
    const taskId = this.generateTaskId();
    if (options.key) this.taskKeys.add(options.key);

    const controller = new AbortController();

    const newTask = {
      id: taskId,
      taskFn,
      priority: basePriority,
      callback,
      key: options.key,
      timeout: options.timeout || this.defaultTimeout,
      retriesLeft:
        options.retries !== undefined ? options.retries : this.maxRetries,
      expiresAt: Date.now() + (options.ttl || 30000),
      createdAt: Date.now(),
      tags: options.tags || [],
      dependencies: options.dependencies || [],
      workflow: options.workflow || null,
      step: options.step || null,
      controller,
    };

    let inserted = false;
    for (let i = 0; i < this.queue.length; i++) {
      if (newTask.priority < this.queue[i].priority) {
        this.queue.splice(i, 0, newTask);
        inserted = true;
        break;
      }
    }
    if (!inserted) this.queue.push(newTask);

    this.recordRate(options.key || null);

    this.addToAuditLog(
      taskId,
      "QUEUED",
      `Priority: ${options.priority || "normal"}`,
    );

    if (this.storageAdapter && this.storageAdapter.saveTask) {
      this.storageAdapter.saveTask(newTask, this.nodeId);
    }

    this.applyPlugins("onTaskQueued", { task: newTask });
    this.processNext();
    return taskId;
  }

  addBulkTasks(tasks) {
    const ids = [];
    for (const t of tasks) {
      const id = this.addTask(t.taskFn, t.options || {}, t.callback);
      ids.push(id);
    }
    return ids;
  }

  findRunnableTaskIndex() {
    const now = Date.now();
    let bestIndex = -1;
    let bestScore = Infinity;
    for (let i = 0; i < this.queue.length; i++) {
      const t = this.queue[i];
      if (t.dependencies && t.dependencies.length > 0) {
        const allDone = t.dependencies.every((dep) =>
          this.completedTasks.has(dep),
        );
        if (!allDone) continue;
      }
      const waitTime = now - t.createdAt;
      const agingBoost = Math.floor(waitTime / 5000);
      const effectivePriority = t.priority - agingBoost;
      if (effectivePriority < bestScore) {
        bestScore = effectivePriority;
        bestIndex = i;
      }
    }
    return bestIndex;
  }

  async processNext() {
    const currentConcurrency =
      this.queue.length > 20 ? this.maxConcurrency : this.baseConcurrency;

    if (
      this.isPaused ||
      this.activeCount >= currentConcurrency ||
      this.queue.length === 0
    ) {
      if (this.activeCount === 0 && this.queue.length === 0) {
        this.emit("onDrain");
        if (this.isShuttingDown) this.emit("onShutdown");
      }
      return;
    }

    const index = this.findRunnableTaskIndex();
    if (index === -1) return;

    this.activeCount++;
    let currentTask = this.queue.splice(index, 1)[0];

    if (
      Date.now() - currentTask.createdAt > 10000 &&
      currentTask.priority > 1
    ) {
      currentTask.priority = 1;
      this.addToAuditLog(
        currentTask.id,
        "ESCALATED",
        "Priority escalated automatically.",
      );
    }

    if (Date.now() > currentTask.expiresAt) {
      this.addToAuditLog(currentTask.id, "EXPIRED");
      this.log("warn", `Task [${currentTask.id}] expired in queue.`);
      this.cleanupTask(currentTask);
      this.activeCount--;
      this.processNext();
      return;
    }

    this.emit("onTaskStart", { taskId: currentTask.id });
    this.applyPlugins("onTaskStart", { task: currentTask });
    this.addToAuditLog(currentTask.id, "STARTED");

    const queueWaitTime = Date.now() - currentTask.createdAt;
    this.metrics.totalWaitTime += queueWaitTime;
    const executionStartTime = Date.now();

    const timeoutPromise = new Promise((_, reject) =>
      setTimeout(() => {
        currentTask.controller.abort();
        reject(new Error(`Execution timeout after ${currentTask.timeout}ms.`));
      }, currentTask.timeout),
    );

    this.activeTasks.set(currentTask.id, currentTask);

    try {
      this.log(
        "process",
        `Executing [${currentTask.id}] (Concurrency: ${this.activeCount}/${currentConcurrency})...`,
      );
      const result = await Promise.race([
        currentTask.taskFn(currentTask.controller.signal),
        timeoutPromise,
      ]);

      this.metrics.successful++;
      this.metrics.totalExecTime += Date.now() - executionStartTime;
      this.consecutiveFailures = 0;
      this.completedTasks.add(currentTask.id);

      this.addToAuditLog(currentTask.id, "SUCCESS");
      this.log(
        "success",
        `Task [${currentTask.id}] completed in ${Date.now() - executionStartTime}ms.`,
      );
      this.emit("onTaskSuccess", { taskId: currentTask.id, result });
      this.applyPlugins("onTaskSuccess", { task: currentTask, result });

      if (currentTask.workflow && currentTask.step) {
        this.advanceWorkflow(currentTask.workflow, currentTask.step, result);
      }

      if (currentTask.callback) currentTask.callback(null, result);
    } catch (error) {
      this.metrics.failed++;
      this.consecutiveFailures++;
      this.log("error", `Task [${currentTask.id}] failed: ${error.message}`);

      this.applyPlugins("onTaskError", { task: currentTask, error });

      if (this.consecutiveFailures >= this.circuitThreshold) {
        this.isCircuitOpen = true;
        this.log(
          "error",
          "CRITICAL: Circuit Breaker Tripped! Entering cooldown mode...",
        );
        setTimeout(() => {
          this.isCircuitOpen = false;
          this.consecutiveFailures = 0;
        }, 30000);
      }

      if (
        currentTask.retriesLeft > 0 &&
        !currentTask.controller.signal.aborted
      ) {
        currentTask.retriesLeft--;
        this.addToAuditLog(
          currentTask.id,
          "RETRYING",
          `Retries left: ${currentTask.retriesLeft}`,
        );

        let inserted = false;
        for (let i = 0; i < this.queue.length; i++) {
          if (currentTask.priority < this.queue[i].priority) {
            this.queue.splice(i, 0, currentTask);
            inserted = true;
            break;
          }
        }
        if (!inserted) this.queue.push(currentTask);
        currentTask = null;
      } else {
        this.addToAuditLog(currentTask.id, "FAILED", error.message);
        this.emit("onTaskFailed", {
          taskId: currentTask.id,
          error: error.message,
        });
        if (currentTask.callback) currentTask.callback(error, null);
      }
    } finally {
      if (currentTask) this.cleanupTask(currentTask);
      this.activeTasks.delete(currentTask?.id);
      this.metrics.totalProcessed++;
      this.activeCount--;
      this.processNext();
    }
  }

  advanceWorkflow(name, stepName, result) {
    const steps = this.workflows.get(name);
    if (!steps) return;
    const index = steps.findIndex((s) => s.name === stepName);
    if (index === -1 || index === steps.length - 1) return;
    const nextStep = steps[index + 1];
    this.addTask(
      nextStep.taskFn,
      {
        workflow: name,
        step: nextStep.name,
        priority: nextStep.priority || "normal",
      },
      nextStep.callback,
    );
  }

  cleanupTask(task) {
    if (task.key) this.taskKeys.delete(task.key);
    if (this.storageAdapter && this.storageAdapter.deleteTask) {
      this.storageAdapter.deleteTask(task.id, this.nodeId);
    }
    task.taskFn = null;
    task.callback = null;
  }

  getMetrics() {
    const avgSuccessTime = this.metrics.successful
      ? (this.metrics.totalExecTime / this.metrics.successful).toFixed(1)
      : 0;
    const avgQueueTime = this.metrics.totalProcessed
      ? (this.metrics.totalWaitTime / this.metrics.totalProcessed).toFixed(1)
      : 0;
    return {
      ...this.metrics,
      averageExecutionTimeMs: `${avgSuccessTime}ms`,
      averageQueueWaitTimeMs: `${avgQueueTime}ms`,
      currentQueueLength: this.queue.length,
      activeConcurrency: this.activeCount,
      heapUsedMegabytes: (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(
        2,
      ),
      nodeId: this.nodeId,
    };
  }

  resetMetrics() {
    this.metrics = {
      totalProcessed: 0,
      successful: 0,
      failed: 0,
      totalWaitTime: 0,
      totalExecTime: 0,
    };
  }

  getQueueSnapshot() {
    return this.queue.map((t) => ({
      id: t.id,
      priority: t.priority,
      key: t.key,
      expiresAt: t.expiresAt,
      createdAt: t.createdAt,
      retriesLeft: t.retriesLeft,
      tags: t.tags,
      dependencies: t.dependencies,
      workflow: t.workflow,
      step: t.step,
    }));
  }

  getTaskById(taskId) {
    const inQueue = this.queue.find((t) => t.id === taskId) || null;
    if (inQueue) return inQueue;
    return this.activeTasks.get(taskId) || null;
  }

  cancelTaskById(taskId) {
    const index = this.queue.findIndex((t) => t.id === taskId);
    if (index !== -1) {
      const task = this.queue[index];
      task.controller.abort();
      this.cleanupTask(task);
      this.queue.splice(index, 1);
      this.addToAuditLog(taskId, "CANCELLED", "Cancelled by ID in queue.");
      return true;
    }
    const active = this.activeTasks.get(taskId);
    if (active) {
      active.controller.abort();
      this.addToAuditLog(taskId, "CANCELLED", "Cancelled active task by ID.");
      return true;
    }
    return false;
  }

  cancelTaskByKey(key) {
    let cancelled = false;
    this.queue = this.queue.filter((t) => {
      if (t.key === key) {
        t.controller.abort();
        this.cleanupTask(t);
        this.addToAuditLog(t.id, "CANCELLED", "Cancelled by key.");
        cancelled = true;
        return false;
      }
      return true;
    });
    for (const [id, task] of this.activeTasks.entries()) {
      if (task.key === key) {
        task.controller.abort();
        this.addToAuditLog(id, "CANCELLED", "Cancelled active task by key.");
        cancelled = true;
      }
    }
    return cancelled;
  }

  flushQueue() {
    for (const task of this.queue) {
      task.controller.abort();
      this.cleanupTask(task);
      this.addToAuditLog(task.id, "FLUSHED", "Queue flushed.");
    }
    this.queue = [];
  }

  setConcurrency(base, max) {
    if (typeof base === "number" && base > 0) this.baseConcurrency = base;
    if (typeof max === "number" && max >= base) this.maxConcurrency = max;
  }

  setMaxQueueLimit(limit) {
    if (typeof limit === "number" && limit > 0) this.maxQueueLimit = limit;
  }

  pause() {
    this.isPaused = true;
    this.log("warn", "Execution Engine Paused.");
  }

  resume() {
    this.isPaused = false;
    this.log("success", "Execution Engine Resumed.");
    this.processNext();
  }

  shutdown() {
    this.isShuttingDown = true;
    this.log("warn", "Shutdown initiated. No new tasks will be accepted.");
  }

  setLoggingEnabled(enabled) {
    this.enableLogging = !!enabled;
  }

  getDashboardData() {
    return {
      metrics: this.getMetrics(),
      queue: this.getQueueSnapshot(),
      auditLogs: this.getAuditLogs(),
      activeTasks: Array.from(this.activeTasks.keys()),
      nodeId: this.nodeId,
    };
  }

  getTraces() {
    return this.traces.slice();
  }

  on(event, callback) {
    if (this.listeners[event]) this.listeners[event].push(callback);
  }

  emit(event, data) {
    if (this.listeners[event]) this.listeners[event].forEach((cb) => cb(data));
  }

  log(type, message) {
    if (!this.enableLogging) return;
    const colors = {
      info: "\x1b[36m",
      process: "\x1b[34m",
      success: "\x1b[32m",
      warn: "\x1b[33m",
      error: "\x1b[31m",
    };
    console.log(
      `${colors[type] || "\x1b[0m"}[${new Date().toLocaleTimeString()}] [${type.toUpperCase()}] ${message}\x1b[0m`,
    );
  }
}

module.exports = UltraBackendEngine;
