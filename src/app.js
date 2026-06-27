const express = require("express");
const cors = require("cors");
const apiRoutes = require("./routes/api.routes");
const errorHandler = require("./middlewares/errorHandler");

const app = express();

app.use(cors());
app.use(express.json());

app.use("/api", apiRoutes);

app.use((req, res) => {
  res.status(404).json({ error: "Route not found" });
});

app.use(errorHandler);

module.exports = app;
