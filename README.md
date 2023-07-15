# Earthquake ETL into DWH and Send GMAIL Notifications.
This project flow is first scrap daily BMKG Data in JSON Format, soured from (data.bmkg.go.id)
then move current reading of Data into temporary table, for purpose to avoid duplication before writing into main table,
after that move data from temporary table into main table by comparing the occurances and date,
finally we check in main table if any earthquake occurs if so then send email else do nothing.

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

