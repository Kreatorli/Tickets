"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TicketUpdatedListener = void 0;
const common_1 = require("@aetickets/common");
const ticket_1 = require("../../models/ticket");
const queue_group_name_1 = require("./queue-group-name");
class TicketUpdatedListener extends common_1.Listener {
    constructor() {
        super(...arguments);
        this.subject = common_1.Subjects.TicketUpdate;
        this.queueGroupName = queue_group_name_1.queueGroupName;
    }
    async onMessage(data, msg) {
        const ticket = await ticket_1.Ticket.findByEvent(data);
        if (!ticket) {
            throw new Error("Ticket not found");
        }
        const { title, price } = data;
        ticket.set({ title, price });
        await ticket.save();
        msg.ack();
    }
}
exports.TicketUpdatedListener = TicketUpdatedListener;
