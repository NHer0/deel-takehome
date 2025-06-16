airflow-all: airflow-build airflow-up airflow-init airflow-create-admin

airflow-build:
	docker-compose build 

airflow-up:
	docker-compose up -d

airflow-init:
	docker-compose run airflow-webserver airflow db init

airflow-create-admin:
	docker-compose run airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin 