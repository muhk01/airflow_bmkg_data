# Earthquake ETL into DWH and Send GMAIL Notifications.

## How to run
to run this project by firing up the docker
```
docker compose up -d
```

## Setup Environment Variable
Copy paste variable.json into Airflow UI at "Admin" - "Variables"

## Database Connection
Setup connection for PostgreSQL in purpose for using PostgresOperator in Airflow UI at 

## SMTP Authentication Setup
This case is using Gmail Services, make sure setup two factor auth, and enable "App Password" Option

## Result
This case suppose more than earthquake is occur in a day.

