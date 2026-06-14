.PHONY: demo clean logs seed shell-agent shell-kafka status

demo:
	@echo "Starting Kafka Operations Agent..."
	@cp -n .env.example .env 2>/dev/null || true
	@echo "Make sure ANTHROPIC_API_KEY and OPENAI_API_KEY are set in .env"
	docker compose up --build

demo-detached:
	docker compose up --build -d

clean:
	docker compose down -v --remove-orphans
	docker volume prune -f

logs:
	docker compose logs -f kafka-ops-agent

logs-all:
	docker compose logs -f

seed:
	docker compose run --rm knowledge-seeder

shell-agent:
	docker compose exec kafka-ops-agent bash

shell-kafka:
	docker compose exec kafka bash

status:
	docker compose ps

topics:
	docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

lag:
	docker compose exec kafka kafka-consumer-groups \
		--bootstrap-server localhost:9092 \
		--describe --all-groups

stop:
	docker compose stop

restart-agent:
	docker compose restart kafka-ops-agent
