"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.newOrderRouter = void 0;
const express_1 = __importDefault(require("express"));
const requireAuth_1 = require("../middleware/requireAuth");
const ticket_1 = require("../models/ticket");
const order_1 = require("../models/order");
const order_created_publisher_1 = require("../events/publisher/order-created-publisher");
const nats_wrapper_1 = require("../nats-wrapper");
const router = express_1.default.Router();
exports.newOrderRouter = router;
router.post("/api/orders", requireAuth_1.requireAuth, async (req, res) => {
    const { ticketId } = req.body;
    // check if the ticket is already exist or not
    const ticket = await ticket_1.Ticket.findById(ticketId);
    if (!ticket) {
        return res.status(404).send({ message: "No Ticket found" });
    }
    // check if the ticket is already reserved
    const isReserved = await ticket.isReserved();
    if (isReserved) {
        return res.status(400).send({ message: "Ticket is already reserved" });
    }
    // set the expiration date for the ticket
    const expiration = new Date();
    expiration.setSeconds(expiration.getSeconds() + 15 * 60);
    // Build the ticket order
    const order = order_1.Order.build({
        userId: req.currentUser.userId,
        status: order_1.OrderStatus.Created,
        expiresAt: expiration,
        ticket,
    });
    await order.save();
    //publish created event
    new order_created_publisher_1.OrderCreatedPublisher(nats_wrapper_1.natsWrapper.client).publish({
        id: order.id,
        status: order.status,
        version: order.version,
        userId: order.userId,
        expiresAt: order.expiresAt.toISOString(),
        ticket: {
            id: ticket.id,
            price: ticket.price,
        },
    });
    res.status(201).send(order);
});
