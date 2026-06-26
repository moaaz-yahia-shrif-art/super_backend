const v8 = require("v8");

class UltraBackendEngine {
  constructor(options = {}) {
    this.queue = [];
    this.taskKeys = new Set();
    this.auditLogs = [];
    this.activeCount = 0;
    this.isPaused = false;
    this.consecutiveFailures = 0;
    this.circuitState = "CLOSED";
    this.circuitOpenTime = 0;
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
    this.circuitCooldown = options.circuitCooldown || 30000;
    this.memoryLimit = options.memoryLimit || 0.85;
    this.deadlineTimeout = options.deadlineTimeout || 60000;

    this.metrics = {
      totalProcessed: 0,
      successful: 0,
      failed: 0,
      totalWaitTime: 0,
      totalExecTime: 0,
      circuitBreakerTrips: 0,
    };

    this.listeners = {
      onTaskStart: [],
      onTaskSuccess: [],
      onTaskFailed: [],
      onQueueFull: [],
      onDrain: [],
      onShutdown: [],
      onMemoryWarning: [],
    };
  }

  async initFromStorage() {
    if (!this.storageAdapter || !this.storageAdapter.loadPendingTasks) return;
    const tasks = await this.storageAdapter.loadPendingTasks(this.nodeId);
    for (const t of tasks) {
      this.queue.push(t);
      this.taskKeys.add(t.key);
    }
    this.sortQueue();
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

  sortQueue() {
    this.queue.sort((a, b) => a.priority - b.priority);
  }

  checkMemoryProtection() {
    const mem = process.memoryUsage();
    const heapLimit = v8.getHeapStatistics().heap_size_limit;
    const usageRatio = mem.heapUsed / heapLimit;

    if (usageRatio > this.memoryLimit) {
      this.emit("onMemoryWarning", { usageRatio, heapUsed: mem.heapUsed });
      this.log(
        "warn",
        `Memory usage critical: ${(usageRatio * 100).toFixed(1)}%. Triggering defense.`,
      );

      if (global.gc) {
        global.gc();
        this.log("info", "Forced Garbage Collection executed.");
      }
      return false;
    }
    return true;
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

  detectDeadlock(taskId, dependencies) {
    const visited = new Set();
    const check = (deps) => {
      for (const depId of deps) {
        if (depId === taskId) return true;
        if (visited.has(depId)) continue;
        visited.add(depId);
        const depTask = this.getTaskById(depId);
        if (depTask && depTask.dependencies) {
          if (check(depTask.dependencies)) return true;
        }
      }
      return false;
    };
    return check(dependencies);
  }

  addTask(taskFn, options = {}, callback) {
    if (this.circuitState === "OPEN") {
      if (Date.now() - this.circuitOpenTime > this.circuitCooldown) {
        this.circuitState = "HALF-OPEN";
        this.log(
          "warn",
          "Circuit Breaker enters HALF-OPEN state. Testing system...",
        );
      } else {
        if (callback)
          callback(new Error("Circuit Breaker is OPEN. Task Rejected."), null);
        return null;
      }
    }

    if (this.isShuttingDown || !this.checkMemoryProtection()) {
      if (callback)
        callback(
          new Error(
            "System Protection Mode Active or Shutting down. Task Rejected.",
          ),
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

    if (
      options.dependencies &&
      this.detectDeadlock(options.id, options.dependencies)
    ) {
      if (callback)
        callback(new Error("Deadlock detected in dependencies."), null);
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
      maxRetries:
        options.retries !== undefined ? options.retries : this.maxRetries,
      expiresAt: Date.now() + (options.ttl || 30000),
      createdAt: Date.now(),
      tags: options.tags || [],
      dependencies: options.dependencies || [],
      workflow: options.workflow || null,
      step: options.step || null,
      controller,
    };

    this.queue.push(newTask);
    this.sortQueue();

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
      ids.push(this.addTask(t.taskFn, t.options || {}, t.callback));
    }
    return ids;
  }

  findRunnableTaskIndex() {
    const now = Date.now();
    for (let i = 0; i < this.queue.length; i++) {
      const t = this.queue[i];
      if (t.dependencies && t.dependencies.length > 0) {
        const allDone = t.dependencies.every((dep) =>
          this.completedTasks.has(dep),
        );
        if (!allDone) continue;
      }

      const waitTime = now - t.createdAt;
      if (waitTime > 5000 && t.priority > 1) {
        t.priority--;
        this.addToAuditLog(
          t.id,
          "ESCALATED",
          "Priority escalated via aging boost.",
        );
      }
      return i;
    }
    return -1;
  }

  calculateDynamicConcurrency() {
    const mem = process.memoryUsage();
    const heapLimit = v8.getHeapStatistics().heap_size_limit;
    const currentRamRatio = mem.heapUsed / heapLimit;

    if (currentRamRatio > 0.75) return this.baseConcurrency;
    return this.queue.length > 20 ? this.maxConcurrency : this.baseConcurrency;
  }

  async processNext() {
    const currentConcurrency = this.calculateDynamicConcurrency();

    if (
      this.isPaused ||
      this.activeCount >= currentConcurrency ||
      this.queue.length === 0
    ) {
      if (this.activeCount === 0 && this.queue.length === 0) {
        this.emit("onDrain");
      }
      return;
    }

    const index = this.findRunnableTaskIndex();
    if (index === -1) return;

    let currentTask = this.queue.splice(index, 1)[0];

    if (
      Date.now() - currentTask.createdAt > this.deadlineTimeout ||
      Date.now() > currentTask.expiresAt
    ) {
      this.addToAuditLog(currentTask.id, "EXPIRED/DEADLINE");
      this.log(
        "warn",
        `Task [${currentTask.id}] dropped due to expiration/deadline.`,
      );
      this.cleanupTask(currentTask);
      this.processNext();
      return;
    }

    this.activeCount++;
    this.emit("onTaskStart", { taskId: currentTask.id });
    this.applyPlugins("onTaskStart", { task: currentTask });
    this.addToAuditLog(currentTask.id, "STARTED");

    this.metrics.totalWaitTime += Date.now() - currentTask.createdAt;
    const executionStartTime = Date.now();

    let timeoutId;
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        currentTask?.controller?.abort();
        reject(
          new Error(
            `Execution timeout after ${currentTask?.timeout || this.defaultTimeout}ms.`,
          ),
        );
      }, currentTask.timeout);
    });

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

      if (this.circuitState === "HALF-OPEN") {
        this.circuitState = "CLOSED";
        this.log("success", "System proved stable. Circuit Breaker CLOSED.");
      }

      this.addToAuditLog(currentTask.id, "SUCCESS");
      this.log(
        "success",
        `Task [${currentTask.id}] completed in ${Date.now() - executionStartTime}ms.`,
      );
      this.emit("onTaskSuccess", { taskId: currentTask.id, result });

      if (currentTask.workflow && currentTask.step) {
        this.advanceWorkflow(currentTask.workflow, currentTask.step, result);
      }

      if (currentTask.callback) currentTask.callback(null, result);
    } catch (error) {
      this.metrics.failed++;
      this.consecutiveFailures++;
      this.log("error", `Task [${currentTask.id}] failed: ${error.message}`);

      if (
        this.consecutiveFailures >= this.circuitThreshold &&
        this.circuitState !== "OPEN"
      ) {
        this.circuitState = "OPEN";
        this.circuitOpenTime = Date.now();
        this.metrics.circuitBreakerTrips++;
        this.log(
          "error",
          "CRITICAL: Circuit Breaker Tripped OPEN! Entering cooldown mode...",
        );
      }

      if (
        currentTask.retriesLeft > 0 &&
        !currentTask.controller.signal.aborted
      ) {
        currentTask.retriesLeft--;
        const attempt = currentTask.maxRetries - currentTask.retriesLeft;
        const backoffDelay = Math.pow(2, attempt) * 1000;

        this.addToAuditLog(
          currentTask.id,
          "RETRY_SCHEDULED",
          `Backoff: ${backoffDelay}ms left: ${currentTask.retriesLeft}`,
        );

        setTimeout(() => {
          if (!this.isShuttingDown) {
            this.queue.push(currentTask);
            this.sortQueue();
            this.processNext();
          }
        }, backoffDelay);

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
      clearTimeout(timeoutId);
      if (currentTask) {
        this.cleanupTask(currentTask);
        this.activeTasks.delete(currentTask.id);
      }
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

  serializeState() {
    return JSON.stringify({
      nodeId: this.nodeId,
      queueLength: this.queue.length,
      circuitState: this.circuitState,
      completedTasksCount: this.completedTasks.size,
      metrics: this.metrics,
      snapshot: this.getQueueSnapshot(),
    });
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
      circuitState: this.circuitState,
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
      circuitBreakerTrips: 0,
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
    return (
      this.queue.find((t) => t.id === taskId) ||
      this.activeTasks.get(taskId) ||
      null
    );
  }

  cancelTaskById(taskId) {
    const index = this.queue.findIndex((t) => t.id === taskId);
    if (index !== -1) {
      const task = this.queue[index];
      task.controller.abort();
      this.cleanupTask(task);
      this.queue.splice(index, 1);
      this.addToAuditLog(taskId, "CANCELLED");
      return true;
    }
    const active = this.activeTasks.get(taskId);
    if (active) {
      active.controller.abort();
      this.addToAuditLog(taskId, "CANCELLED");
      return true;
    }
    return false;
  }

  flushQueue() {
    for (const task of this.queue) {
      task.controller.abort();
      this.cleanupTask(task);
    }
    this.queue = [];
    this.taskKeys.clear();
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

  async shutdown(timeoutMs = 15000) {
    this.isShuttingDown = true;
    this.log(
      "warn",
      `Shutdown initiated. Waiting up to ${timeoutMs}ms for ${this.activeCount} active tasks...`,
    );

    const checkDrain = (resolve) => {
      if (this.activeCount === 0) resolve(true);
      else setTimeout(() => checkDrain(resolve), 200);
    };

    const drainPromise = new Promise((resolve) => checkDrain(resolve));
    const timeoutPromise = new Promise((resolve) =>
      setTimeout(() => resolve(false), timeoutMs),
    );

    const cleanDone = await Promise.race([drainPromise, timeoutPromise]);

    if (!cleanDone) {
      this.log(
        "error",
        "Forced shutdown active due to timeout. Aborting remaining tasks.",
      );
      for (const [id, task] of this.activeTasks.entries()) {
        task.controller.abort();
      }
    } else {
      this.log(
        "success",
        "All tasks drained successfully. Safe shutdown complete.",
      );
    }

    this.flushQueue();
    this.listeners.onShutdown.forEach((cb) => cb());
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
