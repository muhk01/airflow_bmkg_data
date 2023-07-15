# Earthquake ETL into DWH and Send GMAIL Notifications.
![alt text](https://raw.githubusercontent.com/muhk01/airflow_bmkg_data/main/img/5.PNG)
This project flow is first scrap daily BMKG Data in JSON Format, sourced from (data.bmkg.go.id)
then move current reading of Data into temporary table, for purpose to avoid duplication before writing into main table,
after that move data from temporary table into main table by comparing the occurances and date,
finally we check in main table if any earthquake occurs if so then send email else do nothing.

## How to run
to run this project by firing up the docker
```
docker compose up -d
```

## Setup Environment Variable
![alt text](https://raw.githubusercontent.com/muhk01/airflow_bmkg_data/main/img/1.PNG)
Copy paste variable.json into Airflow UI at "Admin" - "Variables"

## Database Connection
![alt text](https://raw.githubusercontent.com/muhk01/airflow_bmkg_data/main/img/2.PNG)
Setup connection for PostgreSQL in purpose for using PostgresOperator in Airflow UI at 

## SMTP Authentication Setup
![alt text](https://raw.githubusercontent.com/muhk01/airflow_bmkg_data/main/img/3.PNG)
This case is using Gmail Services, make sure setup two factor auth, and enable "App Password" Option

## Result
![alt text](https://raw.githubusercontent.com/muhk01/airflow_bmkg_data/main/img/4.PNG)
This case suppose more than earthquake is occur in a day.

