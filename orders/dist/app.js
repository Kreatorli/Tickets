"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.app = void 0;
require("express-async-errors");
const express_1 = __importDefault(require("express"));
const morgan_1 = __importDefault(require("morgan"));
const cors_1 = __importDefault(require("cors"));
const cookie_parser_1 = __importDefault(require("cookie-parser"));
const currentUser_1 = require("./middleware/currentUser");
const delete_1 = require("./routes/delete");
const routes_1 = require("./routes");
const new_1 = require("./routes/new");
const show_1 = require("./routes/show");
const app = (0, express_1.default)();
exports.app = app;
app.set("trust proxy", true);
app.use((0, cors_1.default)({
    origin: "*",
}));
app.use(express_1.default.json());
app.use((0, morgan_1.default)("dev"));
app.use((0, cookie_parser_1.default)());
app.use(currentUser_1.currentUser);
app.use(delete_1.deleteOrderRouter);
app.use(routes_1.indexOrderRouter);
app.use(new_1.newOrderRouter);
app.use(show_1.showOrderRouter);
app.get('/health', (req, res) => {
    res.status(200).send('OK');
});
