"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.deleteOrderRouter = void 0;
const express_1 = __importDefault(require("express"));
const order_1 = require("../models/order");
const order_cancelled_publisher_1 = require("../events/publisher/order-cancelled-publisher");
const nats_wrapper_1 = require("../nats-wrapper");
const requireAuth_1 = require("../middleware/requireAuth");
const router = express_1.default.Router();
exports.deleteOrderRouter = router;
router.delete("/api/orders/:orderId", requireAuth_1.requireAuth, async (req, res) => {
    const { orderId } = req.params;
    const order = await order_1.Order.findById(orderId);
    if (!order) {
        return res.status(404).send({ message: "No Order found" });
    }
    if (order.userId !== req.currentUser.userId) {
        return res
            .status(403)
            .send({ message: "un authorized to delete this order" });
    }
    order.status = order_1.OrderStatus.Cancelled;
    await order.save();
    //publish the order is cancelled using natsWrapper
    new order_cancelled_publisher_1.OrderCancelledPublisher(nats_wrapper_1.natsWrapper.client).publish({
        id: order.id,
        version: order.version,
        ticket: {
            id: order.ticket.id,
        },
    });
    res.status(204).send(order);
});
