const v8 = require("v8");
const EventEmitter = require("events");
const Logger = require("./core/logger");
const RateLimiter = require("./core/rateLimiter");
const CircuitBreaker = require("./core/circuitBreaker");
const Metrics = require("./core/metrics");
const WorkflowEngine = require("./core/workflowEngine");
const TaskQueue = require("./core/taskQueue");

class UltraBackendEngine extends EventEmitter {
  constructor(options = {}) {
    super();
    this.logger = new Logger(
      options.enableLogging !== undefined ? options.enableLogging : true,
    );

    this.nodeId =
      options.nodeId || `node_${Math.random().toString(36).substr(2, 5)}`;
    this.distributeStrategy = options.distributeStrategy || "local";
    this.storageAdapter = options.storageAdapter || null;

    this.baseConcurrency = options.concurrency || 2;
    this.maxConcurrency = options.maxConcurrency || 6;
    this.maxQueueLimit = options.maxQueueLimit || 1500;
    this.maxLogLimit = options.maxLogLimit || 500;
    this.defaultTimeout = options.defaultTimeout || 10000;
    this.maxRetries = options.maxRetries || 2;
    this.memoryLimit = options.memoryLimit || 0.85;
    this.deadlineTimeout = options.deadlineTimeout || 60000;
    this.maxCompletedTasksLimit = options.maxCompletedTasksLimit || 10000;

    this.auditLogs = [];
    this.traces = [];
    this.activeCount = 0;
    this.isPaused = false;
    this.isShuttingDown = false;
    this.taskIdCounter = 0;

    this.rateLimiter = new RateLimiter(
      options.globalPerMinute || 0,
      options.perKeyPerMinute || 0,
    );
    this.circuitBreaker = new CircuitBreaker(
      options.circuitBreakerThreshold || 5,
      options.circuitCooldown || 30000,
    );
    this.metrics = new Metrics();
    this.queueManager = new TaskQueue(
      this.maxQueueLimit,
      this.deadlineTimeout,
      this.maxCompletedTasksLimit,
    );
    this.plugins = [];
    this.workflowEngine = new WorkflowEngine(this.addTask.bind(this));
  }

  async initFromStorage() {
    if (!this.storageAdapter || !this.storageAdapter.loadPendingTasks) return;
    const tasks = await this.storageAdapter.loadPendingTasks(this.nodeId);
    for (const t of tasks) {
      this.queueManager.insertTask(t);
      if (t.key) this.queueManager.addKey(t.key);
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
    this.workflowEngine.defineWorkflow(name, steps);
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
    this.logger.log("info", "Audit logs cleared.");
  }

  generateTaskId() {
    this.taskIdCounter += 1;
    return `t_${this.nodeId}_${this.taskIdCounter}`;
  }

  checkMemoryProtection() {
    const mem = process.memoryUsage();
    const heapLimit = v8.getHeapStatistics().heap_size_limit;
    const usageRatio = mem.heapUsed / heapLimit;
    if (usageRatio > this.memoryLimit) {
      this.emit("onMemoryWarning", { usageRatio, heapUsed: mem.heapUsed });
      this.logger.log(
        "warn",
        `Memory usage critical: ${(usageRatio * 100).toFixed(1)}%. Triggering defense.`,
      );
      if (global.gc) {
        global.gc();
        this.logger.log("info", "Forced Garbage Collection executed.");
      }
      return false;
    }
    return true;
  }

  addTask(taskFn, options = {}, callback) {
    if (!this.circuitBreaker.canExecute()) {
      if (callback)
        callback(new Error("Circuit Breaker is OPEN. Task Rejected."), null);
      return null;
    }

    if (this.circuitBreaker.getState() === "HALF-OPEN") {
      this.logger.log(
        "warn",
        "Circuit Breaker enters HALF-OPEN state. Testing system...",
      );
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

    const key = options.key || null;
    if (!this.rateLimiter.canPass(key)) {
      if (callback) callback(new Error("Rate limit exceeded."), null);
      return null;
    }

    if (this.queueManager.hasKey(key)) {
      this.logger.log("warn", `Task blocked. Duplicate key found: ${key}`);
      return null;
    }

    if (
      options.dependencies &&
      this.queueManager.detectDeadlock(
        options.id,
        options.dependencies,
        this.getTaskById.bind(this),
      )
    ) {
      if (callback)
        callback(new Error("Deadlock detected in dependencies."), null);
      return null;
    }

    if (this.queueManager.isNearLimit()) {
      this.logger.log(
        "warn",
        "Backpressure triggered: Slowing down task acceptance.",
      );
      options.ttl = (options.ttl || 30000) - 2000;
    }

    if (!this.queueManager.canAccept()) {
      this.emit("onQueueFull", { currentLength: this.queueManager.length() });
      if (callback) callback(new Error("Queue Limit Exceeded."), null);
      return null;
    }

    const basePriority =
      options.priority === "high" ? 1 : options.priority === "low" ? 3 : 2;
    const taskId = this.generateTaskId();
    if (key) this.queueManager.addKey(key);

    const controller = new AbortController();
    const now = Date.now();

    const newTask = {
      id: taskId,
      taskFn,
      priority: basePriority,
      callback,
      key,
      timeout: options.timeout || this.defaultTimeout,
      retriesLeft:
        options.retries !== undefined ? options.retries : this.maxRetries,
      maxRetries:
        options.retries !== undefined ? options.retries : this.maxRetries,
      expiresAt: now + (options.ttl || 30000),
      createdAt: now,
      tags: options.tags || [],
      dependencies: options.dependencies || [],
      workflow: options.workflow || null,
      step: options.step || null,
      controller,
    };

    this.queueManager.insertTask(newTask);
    this.rateLimiter.record(key);
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

  calculateDynamicConcurrency() {
    const mem = process.memoryUsage();
    const heapLimit = v8.getHeapStatistics().heap_size_limit;
    const memUsage = mem.heapUsed / heapLimit;

    const cpuLoad = require("os").loadavg()[0] / require("os").cpus().length;

    const memFree = 1 - memUsage;
    const cpuFree = 1 - Math.min(cpuLoad, 1);

    const score = memFree * 0.5 + cpuFree * 0.5;

    const target =
      this.baseConcurrency +
      score * (this.maxConcurrency - this.baseConcurrency);

    if (!this._smoothConcurrency)
      this._smoothConcurrency = this.baseConcurrency;

    this._smoothConcurrency = this._smoothConcurrency * 0.7 + target * 0.3;

    return Math.max(
      this.baseConcurrency,
      Math.min(Math.round(this._smoothConcurrency), this.maxConcurrency),
    );
  }

  async processNext() {
    const currentConcurrency = this.calculateDynamicConcurrency();

    if (
      this.isPaused ||
      this.activeCount >= currentConcurrency ||
      this.queueManager.length() === 0
    ) {
      if (this.activeCount === 0 && this.queueManager.length() === 0) {
        this.emit("onDrain");
      }
      return;
    }

    const index = this.queueManager.findRunnableTaskIndex();
    if (index === -1) return;

    let currentTask = this.queueManager.takeTask(index);

    if (this.queueManager.isExpired(currentTask)) {
      this.addToAuditLog(currentTask.id, "EXPIRED/DEADLINE");
      this.logger.log(
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

    this.metrics.recordWaitTime(Date.now() - currentTask.createdAt);
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

    this.queueManager.addActive(currentTask);

    try {
      this.logger.log(
        "process",
        `Executing [${currentTask.id}] (Concurrency: ${this.activeCount}/${currentConcurrency})...`,
      );

      const result = await Promise.race([
        currentTask.taskFn(currentTask.controller.signal),
        timeoutPromise,
      ]);

      this.metrics.recordSuccess();
      this.metrics.recordExecTime(Date.now() - executionStartTime);
      this.circuitBreaker.recordSuccess();

      this.queueManager.addCompleted(currentTask.id);

      if (this.circuitBreaker.getState() === "CLOSED") {
      }

      this.addToAuditLog(currentTask.id, "SUCCESS");
      this.logger.log(
        "success",
        `Task [${currentTask.id}] completed in ${Date.now() - executionStartTime}ms.`,
      );
      this.emit("onTaskSuccess", { taskId: currentTask.id, result });

      if (currentTask.workflow && currentTask.step) {
        this.workflowEngine.advance(
          currentTask.workflow,
          currentTask.step,
          result,
        );
      }

      if (currentTask.callback) currentTask.callback(null, result);
    } catch (error) {
      this.metrics.recordFailure();
      this.circuitBreaker.recordFailure();
      if (this.circuitBreaker.getState() === "OPEN") {
        this.metrics.recordCircuitTrip();
        this.logger.log(
          "error",
          "CRITICAL: Circuit Breaker Tripped OPEN! Entering cooldown mode...",
        );
      }

      this.logger.log(
        "error",
        `Task [${currentTask.id}] failed: ${error.message}`,
      );

      if (
        currentTask.retriesLeft > 0 &&
        !currentTask.controller.signal.aborted
      ) {
        currentTask.retriesLeft--;
        const attempt = currentTask.maxRetries - currentTask.retriesLeft;
        const baseDelay = Math.pow(2, attempt) * 1000;
        const jitter = Math.random() * 300;
        const backoffDelay = baseDelay + jitter;

        this.addToAuditLog(
          currentTask.id,
          "RETRY_SCHEDULED",
          `Backoff: ${backoffDelay}ms left: ${currentTask.retriesLeft}`,
        );

        const taskToRetry = currentTask;
        currentTask = null;
        this.queueManager.removeActive(taskToRetry.id);

        setTimeout(() => {
          if (!this.isShuttingDown) {
            this.queueManager.insertTask(taskToRetry);
            this.processNext();
          }
        }, backoffDelay);
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
        this.queueManager.removeActive(currentTask.id);
      }
      this.activeCount--;
      this.processNext();
    }
  }

  cleanupTask(task) {
    this.queueManager.removeKey(task.key);
    if (this.storageAdapter && this.storageAdapter.deleteTask) {
      this.storageAdapter.deleteTask(task.id, this.nodeId);
    }
    task.taskFn = null;
    task.callback = null;
  }

  serializeState() {
    return JSON.stringify({
      nodeId: this.nodeId,
      queueLength: this.queueManager.length(),
      circuitState: this.circuitBreaker.getState(),
      completedTasksCount: this.queueManager.completedTasks.size,
      metrics: {
        totalProcessed: this.metrics.totalProcessed,
        successful: this.metrics.successful,
        failed: this.metrics.failed,
        totalWaitTime: this.metrics.totalWaitTime,
        totalExecTime: this.metrics.totalExecTime,
        circuitBreakerTrips: this.metrics.circuitBreakerTrips,
      },
      snapshot: this.queueManager.snapshot(),
    });
  }

  getMetrics() {
    return this.metrics.snapshot(
      this.queueManager.length(),
      this.activeCount,
      this.circuitBreaker.getState(),
      this.nodeId,
    );
  }

  resetMetrics() {
    this.metrics.reset();
  }

  getQueueSnapshot() {
    return this.queueManager.snapshot();
  }

  getTaskById(taskId) {
    return this.queueManager.getTaskById(taskId);
  }

  cancelTaskById(taskId) {
    const cancelled = this.queueManager.cancelTaskById(taskId);
    if (cancelled) {
      this.addToAuditLog(taskId, "CANCELLED");
    }
    return cancelled;
  }

  flushQueue() {
    this.queueManager.flush();
  }

  pause() {
    this.isPaused = true;
    this.logger.log("warn", "Execution Engine Paused.");
  }

  resume() {
    this.isPaused = false;
    this.logger.log("success", "Execution Engine Resumed.");
    this.processNext();
  }

  async shutdown(timeoutMs = 15000) {
    this.isShuttingDown = true;
    this.logger.log(
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
      this.logger.log(
        "error",
        "Forced shutdown active due to timeout. Aborting remaining tasks.",
      );
      for (const [id, task] of this.queueManager.activeTasks.entries()) {
        task.controller.abort();
      }
    } else {
      this.logger.log(
        "success",
        "All tasks drained successfully. Safe shutdown complete.",
      );
    }

    this.flushQueue();
    this.emit("onShutdown");
  }
}

module.exports = UltraBackendEngine;
