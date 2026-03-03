"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.natsWrapper = void 0;
const node_nats_streaming_1 = __importDefault(require("node-nats-streaming"));
class NatsWrapper {
    get client() {
        if (!this._client) {
            throw new Error("Cann't access NATS client before connecting");
        }
        return this._client;
    }
    connect(clusterId, clientId, url) {
        this._client = node_nats_streaming_1.default.connect(clusterId, clientId, { url });
        return new Promise((resolve, reject) => {
            var _a, _b;
            (_a = this._client) === null || _a === void 0 ? void 0 : _a.on("connect", () => {
                console.log("connected to NATS 🔊 📩");
                resolve();
            });
            (_b = this._client) === null || _b === void 0 ? void 0 : _b.on("error", (err) => {
                reject(err);
            });
        });
    }
}
exports.natsWrapper = new NatsWrapper();
