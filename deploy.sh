#!/bin/bash
set -e

echo "=================================================================="
echo "🚀 CampusGrid Single-VM Deployment Script"
echo "=================================================================="

# Check for .env file
if [ ! -f .env ]; then
  echo "❌ Error: .env file not found!"
  echo "Please create a .env file in this directory based on infra/.env.example"
  echo "Make sure to set strong passwords/secrets and add GEMINI_API_KEY."
  exit 1
fi

echo "⚙️  Verifying docker installation..."
if ! command -v docker &> /dev/null; then
  echo "❌ Error: Docker is not installed on this machine."
  echo "Please install Docker and Docker Compose before running this script."
  exit 1
fi

echo "🏗️  Step 1: Building all Docker services (Next.js, FastAPI, etc.)..."
docker compose -f docker-compose.prod.yml build --no-cache

echo "💾  Step 2: Starting Database, Redis, and Object Storage (Infra dependencies)..."
docker compose -f docker-compose.prod.yml up -d postgres redis minio

echo "⏱️  Step 3: Waiting for database to be healthy..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' campugrid-postgres)" == "healthy" ]; do
    echo "⌛ Waiting for PostgreSQL container..."
    sleep 2
done

echo "🔄  Step 4: Running Alembic Database Migrations..."
docker compose -f docker-compose.prod.yml run --rm server alembic upgrade head

echo "🔥  Step 5: Starting remaining services (server, workers, web, proxy)..."
docker compose -f docker-compose.prod.yml up -d

echo "=================================================================="
echo "🎉 Deployment Complete!"
echo "=================================================================="
echo "Services are running:"
echo "👉 Frontend & API Gateway: https://campusgrid.sahuja.in"
echo "👉 Object Storage API:     https://s3.campusgrid.sahuja.in"
echo "👉 MinIO Web Console:      http://<vm-ip>:9001"
echo "👉 Grafana Dashboard:      http://<vm-ip>:3001"
echo ""
echo "Monitor logs using:"
echo "   docker compose -f docker-compose.prod.yml logs -f"
echo "=================================================================="
