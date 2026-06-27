class WorkflowEngine {
  constructor(addTaskFn) {
    this.workflows = new Map();
    this.addTaskFn = addTaskFn;
  }

  defineWorkflow(name, steps) {
    this.workflows.set(name, steps);
  }

  advance(name, stepName, result) {
    const steps = this.workflows.get(name);
    if (!steps) return;
    const index = steps.findIndex((s) => s.name === stepName);
    if (index === -1 || index === steps.length - 1) return;
    const nextStep = steps[index + 1];
    this.addTaskFn(
      nextStep.taskFn,
      {
        workflow: name,
        step: nextStep.name,
        priority: nextStep.priority || "normal",
      },
      nextStep.callback,
    );
  }

  getWorkflows() {
    return this.workflows;
  }
}

module.exports = WorkflowEngine;
