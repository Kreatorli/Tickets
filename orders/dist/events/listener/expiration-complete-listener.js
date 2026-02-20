"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ExpirationCompleteListener = void 0;
const common_1 = require("@aetickets/common");
const queue_group_name_1 = require("./queue-group-name");
const order_1 = require("../../models/order");
const order_cancelled_publisher_1 = require("../publisher/order-cancelled-publisher");
class ExpirationCompleteListener extends common_1.Listener {
    constructor() {
        super(...arguments);
        this.subject = common_1.Subjects.ExpirationComplete;
        this.queueGroupName = queue_group_name_1.queueGroupName;
    }
    async onMessage(data, msg) {
        const order = await order_1.Order.findById(data.orderId).populate("ticket");
        if (!order) {
            throw new Error("Order not found");
        }
        if (order.status === common_1.OrderStatus.Complete) {
            return msg.ack();
        }
        order.set({
            status: common_1.OrderStatus.Cancelled,
        });
        await order.save();
        await new order_cancelled_publisher_1.OrderCancelledPublisher(this.cilent).publish({
            id: order.id,
            version: order.version,
            ticket: {
                id: order.ticket.id,
            },
        });
        msg.ack();
    }
}
exports.ExpirationCompleteListener = ExpirationCompleteListener;
