import express from "express";
import { OrderCreatedListener } from "./events/listeners/order-created-listener";
import { natsWrapper } from "./nats-wrapper";
import { expirationQueue } from "./queues/expiration-queue";

const app = express();

// ✅ Health Route
app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

// Start health server
app.listen(3000, () => {
  console.log("Expiration health server running on port 3000 🚀");
});

const start = async () => {
  if (!process.env.NATS_CLIENT_ID) {
    throw new Error("NATS_CLIENT_ID must be defined");
  }
  if (!process.env.NATS_URL) {
    throw new Error("NATS_URL must be defined");
  }
  if (!process.env.NATS_CLUSTER_ID) {
    throw new Error("NATS_CLUSTER_ID must be defined");
  }

  try {
    await natsWrapper.connect(
      process.env.NATS_CLUSTER_ID,
      process.env.NATS_CLIENT_ID,
      process.env.NATS_URL
    );

    natsWrapper.client.on("close", () => {
      console.log("NATS connection closed!");
      process.exit();
    });
    process.on("SIGINT", () => natsWrapper.client.close());
    process.on("SIGTERM", () => natsWrapper.client.close());

    new OrderCreatedListener(natsWrapper.client).listen();
  } catch (err) {
    console.error(err);
    return;
  }

  setTimeout(async () => {
    console.log("🧪 Test: Adding job to expiration queue (5s delay)...");
    await expirationQueue.add(
      { orderId: "test-order-123" },
      { delay: 5000 }
    );
    console.log("🧪 Test job added. Waiting...");
  }, 2000);
};

start();
