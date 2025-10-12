# Makefile for ETL Service

.PHONY: up down refresh logs test

# Starts the application in detached mode
up:
	@echo "Starting up services..."
	docker-compose up -d --build

# Stops and removes the containers
down:
	@echo "Stopping services..."
	docker-compose down

# Triggers a manual ETL refresh
refresh:
	@echo "Triggering ETL refresh..."
	@docker-compose exec api curl -s -X POST http://localhost:3000/api/refresh \
	  -H "Content-Type: application/json" \
	  -H "Authorization: Bearer $$(grep SECRET_REFRESH_TOKEN .env | cut -d '=' -f2)"

# Follows the logs of the API container
logs:
	@echo "Following API logs..."
	docker-compose logs -f api

# Runs a health check
test:
	@echo "Running health check..."
	@curl -s http://localhost:3000/health |-
