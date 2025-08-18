DB_USER ?= default
DB_PASS ?= default_pass
DB_NAME ?= barn_db


OUTPUT_FILE_ENV = ./config/.env
OUTPUT_FILE_YAML = ./config/cfg.yaml

prepare:
	@echo "POSTGRES_USER=$(DB_USER)" > $(OUTPUT_FILE_ENV)
	@echo "POSTGRES_PASSWORD=$(DB_PASS)" >> $(OUTPUT_FILE_ENV)
	@echo "POSTGRES_DB=$(DB_NAME)" >> $(OUTPUT_FILE_ENV)
	@echo "bucket_service:" > $(OUTPUT_FILE_YAML)
	@echo "  address: 0.0.0.0:50051" >> $(OUTPUT_FILE_YAML)
	@echo "  mask_address: bucket_service:50051" >> $(OUTPUT_FILE_YAML)
	@echo "  log_level: -4" >> $(OUTPUT_FILE_YAML)
	@echo >> $(OUTPUT_FILE_YAML)
	@echo "auth_service:" >> $(OUTPUT_FILE_YAML)
	@echo "  address: 0.0.0.0:50052" >> $(OUTPUT_FILE_YAML)
	@echo "  mask_address: auth_service:50052" >> $(OUTPUT_FILE_YAML)
	@echo "  log_level: -4" >> $(OUTPUT_FILE_YAML)
	@echo >> $(OUTPUT_FILE_YAML)
	@echo "object_service:" >> $(OUTPUT_FILE_YAML)
	@echo "  address: 0.0.0.0:50053" >> $(OUTPUT_FILE_YAML)
	@echo "  mask_address: object_service:50053" >> $(OUTPUT_FILE_YAML)
	@echo "  log_level: -4" >> $(OUTPUT_FILE_YAML)
	@echo >> $(OUTPUT_FILE_YAML)
	@echo "gateway:" >> $(OUTPUT_FILE_YAML)
	@echo "  address: 0.0.0.0:8080" >> $(OUTPUT_FILE_YAML)
	@echo >> $(OUTPUT_FILE_YAML)
	@echo "storage:" >> $(OUTPUT_FILE_YAML)
	@echo "  root: \"/app/data\"" >> $(OUTPUT_FILE_YAML)
	@echo >> $(OUTPUT_FILE_YAML)
	@echo "postgres:" >> $(OUTPUT_FILE_YAML)
	@echo "  address: postgres:5432" >> $(OUTPUT_FILE_YAML)
	@echo "  user: $(DB_USER)" >> $(OUTPUT_FILE_YAML)
	@echo "  password: $(DB_PASS)" >> $(OUTPUT_FILE_YAML)
	@echo "  db: $(DB_NAME)" >> $(OUTPUT_FILE_YAML)

	