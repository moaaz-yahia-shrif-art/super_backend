const app = require("./app");

const PORT = process.env.PORT || 3000;

const server = app.listen(PORT);

server
  .on("error", (err) => {
    if (err.code === "EADDRINUSE") {
      console.log(`Port ${PORT} is in use. Retrying...`);
      setTimeout(() => {
        server.close();
        server.listen(0);
      }, 1000);
    } else {
      console.error("Server error:", err);
    }
  })
  .on("listening", () => {
    const address = server.address();
    const bind = typeof address === "string" ? address : address.port;
    console.log("🚀 Universal Gateway API running on port", bind);
  });
