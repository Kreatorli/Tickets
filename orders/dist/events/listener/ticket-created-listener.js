"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TicketCreatedListener = void 0;
const common_1 = require("@aetickets/common");
const queue_group_name_1 = require("./queue-group-name");
const ticket_1 = require("../../models/ticket");
class TicketCreatedListener extends common_1.Listener {
    constructor() {
        super(...arguments);
        this.subject = common_1.Subjects.TicketCreated;
        this.queueGroupName = queue_group_name_1.queueGroupName;
    }
    async onMessage(data, msg) {
        const { id, title, price } = data;
        const ticket = ticket_1.Ticket.build({
            id,
            title,
            price,
        });
        await ticket.save();
        msg.ack();
    }
}
exports.TicketCreatedListener = TicketCreatedListener;
