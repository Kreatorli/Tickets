"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.showOrderRouter = void 0;
const express_1 = __importDefault(require("express"));
const order_1 = require("../models/order");
const requireAuth_1 = require("../middleware/requireAuth");
const router = express_1.default.Router();
exports.showOrderRouter = router;
router.get("/api/orders/:orderId", requireAuth_1.requireAuth, async (req, res) => {
    const order = await order_1.Order.findById(req.params.orderId).populate("ticket");
    if (!order) {
        return res.status(404).send({ message: "No order found" });
    }
    if (order.userId !== req.currentUser.userId) {
        return res
            .status(403)
            .send({ message: "un authorized to get this order" });
    }
    res.send(order);
});
