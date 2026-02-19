import "express-async-errors";
import express from "express";
import morgan from "morgan";
import cors from "cors";
import cookieSession from "cookie-session";

import { currentUser } from "./middleware/current-user";
import { createTicketRouter } from "./routes/newTicket";
import { showTicketRouter } from "./routes/showTicket";
import { updateTicketRouter } from "./routes/updateTicket";
import { getTicketsRouter } from "./routes/getTickets";

const app = express();

app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

app.set("trust proxy", true);

app.use(
  cors({
    origin: "*",
  })
);

app.use(express.json());
app.use(morgan("dev"));

// ✅ THIS is required to read "session" cookie created by auth
app.use(
  cookieSession({
    signed: false,
    secure: false, // ✅ for localhost + Postman
  })
);

app.use(currentUser);

app.use(createTicketRouter);
app.use(showTicketRouter);
app.use(updateTicketRouter);
app.use(getTicketsRouter);

export { app };
