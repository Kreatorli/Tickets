# =========================
# Stage 1: Build
# =========================
FROM node:19-alpine3.16 AS build

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install ALL dependencies (including dev)
RUN npm install

# Copy source code
COPY . .

# Build TypeScript -> dist/
RUN npm run build


# =========================
# Stage 2: Runtime
# =========================
FROM node:19-alpine3.16 AS runtime

WORKDIR /app

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

# Copy only production dependencies
COPY --from=build /app/package*.json ./
RUN npm install --omit=dev

# Copy compiled output
COPY --from=build /app/dist ./dist

USER nodejs

EXPOSE 3000

CMD ["node", "dist/index.js"]
