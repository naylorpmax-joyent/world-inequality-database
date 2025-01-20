# OPTS := --host=${WID_DB_HOST} --port=${WID_DB_PORT} --user=${WID_DB_USER} --password=${WID_DB_PASS}

clean:
	docker compose down
	docker volume rm wid_db-wid 2> /dev/null || true

db: clean
	docker compose up -d

migrate:
	./bin/wid migrate down
	./bin/wid migrate up

load: migrate
	./bin/wid load raw

build:
	go build -o ./bin/wid cmd/main.go
