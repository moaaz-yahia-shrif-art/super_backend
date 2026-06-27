const express = require("express");
const router = express.Router();
const controller = require("../controllers/engine.controller");

router.post("/execute", controller.executeAction);
router.get("/engine/dashboard", controller.dashboard);

module.exports = router;
