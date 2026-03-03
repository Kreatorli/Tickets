# Stage 1: Build
FROM node:19-alpine3.16 AS build

WORKDIR /app

COPY package*.json ./

# Install all dependencies (including dev)
RUN npm install

COPY . .

# Compile TypeScript
RUN npm run build


# Stage 2: Runtime
FROM node:19-alpine3.16 AS runtime

WORKDIR /app

RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

# Copy compiled output
COPY --from=build --chown=nodejs:nodejs /app/dist ./dist
COPY --from=build --chown=nodejs:nodejs /app/package*.json ./
COPY --from=build --chown=nodejs:nodejs /app/node_modules ./node_modules

USER nodejs

EXPOSE 3000

CMD ["node", "dist/index.js"]
