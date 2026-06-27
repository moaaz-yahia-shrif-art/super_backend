class TaskQueue {
  constructor(maxQueueLimit, deadlineTimeout, maxCompletedTasksLimit) {
    this.queue = [];
    this.taskKeys = new Set();
    this.completedTasks = new Set();
    this.activeTasks = new Map();
    this.maxQueueLimit = maxQueueLimit;
    this.deadlineTimeout = deadlineTimeout;
    this.maxCompletedTasksLimit = maxCompletedTasksLimit;
  }

  insertTask(task) {
    let low = 0;
    let high = this.queue.length;
    while (low < high) {
      let mid = (low + high) >>> 1;
      if (this.queue[mid].priority <= task.priority) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    this.queue.splice(low, 0, task);
  }

  canAccept() {
    return this.queue.length < this.maxQueueLimit;
  }

  isNearLimit() {
    return this.queue.length > this.maxQueueLimit * 0.8;
  }

  addKey(key) {
    if (key) this.taskKeys.add(key);
  }

  hasKey(key) {
    return key && this.taskKeys.has(key);
  }

  removeKey(key) {
    if (key) this.taskKeys.delete(key);
  }

  addCompleted(id) {
    if (this.completedTasks.size >= this.maxCompletedTasksLimit) {
      const firstKey = this.completedTasks.values().next().value;
      this.completedTasks.delete(firstKey);
    }
    this.completedTasks.add(id);
  }

  isDependencySatisfied(deps) {
    if (!deps || deps.length === 0) return true;
    return deps.every((dep) => this.completedTasks.has(dep));
  }

  detectDeadlock(taskId, dependencies, getTaskByIdFn) {
    const visited = new Set();
    const check = (deps) => {
      for (const depId of deps) {
        if (depId === taskId) return true;
        if (visited.has(depId)) continue;
        visited.add(depId);
        const depTask = getTaskByIdFn(depId);
        if (depTask && depTask.dependencies) {
          if (check(depTask.dependencies)) return true;
        }
      }
      return false;
    };
    return check(dependencies);
  }

  findRunnableTaskIndex() {
    const now = Date.now();
    for (let i = 0; i < this.queue.length; i++) {
      const t = this.queue[i];
      if (!t) continue;

      if (!this.isDependencySatisfied(t.dependencies)) continue;

      const waitTime = now - t.createdAt;
      if (waitTime > 5000 && t.priority > 1) {
        this.queue.splice(i, 1);
        t.priority--;
        this.insertTask(t);
        i--;
        continue;
      }
      return i;
    }
    return -1;
  }

  takeTask(index) {
    return this.queue.splice(index, 1)[0];
  }

  isExpired(task, now = Date.now()) {
    if (now - task.createdAt > this.deadlineTimeout) return true;
    if (now > task.expiresAt) return true;
    return false;
  }

  addActive(task) {
    this.activeTasks.set(task.id, task);
  }

  removeActive(id) {
    this.activeTasks.delete(id);
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
      this.queue.splice(index, 1);
      return true;
    }
    const active = this.activeTasks.get(taskId);
    if (active) {
      active.controller.abort();
      return true;
    }
    return false;
  }

  flush() {
    for (const task of this.queue) {
      task.controller.abort();
    }
    this.queue = [];
    this.taskKeys.clear();
  }

  snapshot() {
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

  length() {
    return this.queue.length;
  }
}

module.exports = TaskQueue;
