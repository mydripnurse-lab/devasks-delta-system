// server.js
import "dotenv/config";
import express from "express";
import { oauthRouter } from "./routes/oauth.routes.js";
import { ghlRouter } from "./routes/ghl.routes.js";

const app = express();

// Para JSON requests (POST /ghl/locations)
app.use(express.json({ limit: "2mb" }));

app.get("/", (_req, res) => {
    res.type("html").send(`
    <h1>GHL OAuth + API Test</h1>
    <ul>
      <li><a href="/connect/ghl">Start OAuth</a></li>
      <li><a href="/tokens">View tokens</a></li>
      <li><a href="/ghl/me">GET /oauth/me</a></li>
    </ul>
  `);
});

// OAuth routes
app.use("/", oauthRouter);

// GHL API routes
app.use("/", ghlRouter);

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
    console.log(`✅ Server running: http://localhost:${PORT}`);
    console.log(`➡️ Start OAuth:   http://localhost:${PORT}/connect/ghl`);
});
