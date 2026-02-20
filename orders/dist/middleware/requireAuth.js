"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.requireAuth = void 0;
const requireAuth = (req, res, next) => {
    if (!req.currentUser) {
        return res.status(401).send({ message: "un authenticated" });
    }
    next();
};
exports.requireAuth = requireAuth;
