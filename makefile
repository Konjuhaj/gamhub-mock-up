.PHONY: up down infra test loadtest chaos

TF_DIR := infra/terraform
ENDPOINT := http://localhost:4566

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

infra:
	cd $(TF_DIR) && terraform init -input=false
	cd $(TF_DIR) && AWS_ENDPOINT_URL=$(ENDPOINT) lstk tf apply -auto-approve

test:
	go test ./...

loadtest:
	@echo "TODO: PER-99"

chaos:
	@echo "TODO: PER-103"


