const express = require("express");
const UltraBackendEngine = require("./smartEngine");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

const engine = new UltraBackendEngine({
  concurrency: 2,
  maxConcurrency: 6,
  maxQueueLimit: 500,
  globalPerMinute: 120,
  memoryLimit: 0.9,
});

const mockDatabase = {
  users: [
    { id: 1, name: "Ahmed", role: "admin" },
    { id: 2, name: "Sara", role: "user" },
  ],
};

const actionsRegistry = {
  getData: async (data) => {
    const { resource, id } = data;
    if (!mockDatabase[resource])
      throw new Error(`Resource [${resource}] not found`);

    if (id) {
      const item = mockDatabase[resource].find((u) => u.id === parseInt(id));
      if (!item) throw new Error(`Item with ID [${id}] not found`);
      return item;
    }
    return mockDatabase[resource];
  },

  postData: async (data) => {
    const { resource, payload } = data;
    if (!mockDatabase[resource])
      throw new Error(`Resource [${resource}] not found`);
    if (!payload || !payload.name)
      throw new Error("Invalid payload: name is required");

    const newItem = {
      id: mockDatabase[resource].length + 1,
      ...payload,
    };
    mockDatabase[resource].push(newItem);
    return newItem;
  },

  deleteData: async (data) => {
    const { resource, id } = data;
    if (!mockDatabase[resource])
      throw new Error(`Resource [${resource}] not found`);

    const index = mockDatabase[resource].findIndex(
      (u) => u.id === parseInt(id),
    );
    if (index === -1)
      throw new Error(`Item with ID [${id}] not found to delete`);

    const deletedItem = mockDatabase[resource].splice(index, 1)[0];
    return { message: "Item deleted successfully", deletedItem };
  },
};

app.post("/api/execute", (req, res) => {
  const { action, data, priority, key, timeout } = req.body;

  const targetFunction = actionsRegistry[action];
  if (!targetFunction) {
    return res
      .status(400)
      .json({
        error: `The action [${action}] is not registered in the system.`,
      });
  }

  engine.addTask(
    () => targetFunction(data),
    { priority: priority || "normal", key, timeout },
    (err, result) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json({ status: "success", action, result });
    },
  );
});

app.get("/api/engine/dashboard", (req, res) => {
  res.json(engine.getDashboardData());
});

app.listen(PORT, () => {
  console.log(`🚀 Universal Gateway API is live on port ${PORT}`);
});
