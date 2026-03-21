# ── Stage 1: Build React frontend ────────────────────────────────────────────
FROM node:18-alpine AS frontend-builder

WORKDIR /frontend

ARG REACT_APP_BACKEND_URL=https://web-production-4b6d21.up.railway.app
ARG REACT_APP_API_URL=https://web-production-4b6d21.up.railway.app
ENV REACT_APP_BACKEND_URL=$REACT_APP_BACKEND_URL
ENV REACT_APP_API_URL=$REACT_APP_API_URL

# Clone the frontend repo and build
RUN apk add --no-cache git && \
    git clone https://github.com/vipinkarthic24/sv-fincloud-frontend.git .

# Use npm (avoids yarn.lock mismatch issues)
RUN npm install --legacy-peer-deps
RUN npm run build

# ── Stage 2: Python / FastAPI backend ────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend source
COPY . .

# Place the React build where server.py expects it: /app/frontend/build
COPY --from=frontend-builder /frontend/build ./frontend/build

EXPOSE 8000

# Use shell form so $PORT is expanded at runtime
CMD gunicorn -w 1 -k uvicorn.workers.UvicornWorker server:app --bind 0.0.0.0:${PORT:-8000}
