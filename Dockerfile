# Multi-stage Dockerfile for Kill It Twice replication pipeline & UI

# Stage 1: Build Frontend UI
FROM node:20-alpine AS ui-builder
WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm install
COPY ui/ ./
RUN npm run build

# Stage 2: Build Backend Engine
FROM node:20-alpine AS backend-builder
WORKDIR /app
COPY package*.json tsconfig.json ./
RUN npm install
COPY src/ ./src
RUN npm run build

# Stage 3: Production Runtime (Bounded to <= 512MB RAM)
FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production

COPY package*.json ./
RUN npm install --only=production

# Copy compiled backend
COPY --from=backend-builder /app/dist ./dist

# Copy compiled frontend SPA
COPY --from=ui-builder /app/ui/dist ./ui/dist

# Expose HTTP API & UI Port
EXPOSE 4000

CMD ["node", "dist/index.js"]
