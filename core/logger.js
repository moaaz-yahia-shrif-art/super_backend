class Logger {
  constructor(enableLogging = true) {
    this.enableLogging = enableLogging;
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

module.exports = Logger;
