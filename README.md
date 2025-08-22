
# Stock Data Assignment

## Overview
This project implements a dockerized ETL pipeline using Apache Airflow and Postgres to fetch, process, and store real-time stock data from the Alpha Vantage API. The pipeline is fully automated and orchestrated with Airflow, and all services run in containers for easy reproducibility.

## Architecture
- **Postgres**: Stores the stock data in a table (`stock_prices`).
- **Airflow**: Orchestrates the ETL pipeline (extract, transform, load) as a DAG.
- **Python Scripts**: Handle API requests, data transformation, and database insertion.
- **Docker Compose**: Manages all services and their dependencies.

## Setup Instructions
1. **Clone the repository**
2. **Set your Alpha Vantage API key**
	- The API key is set in `docker-compose.yml` as `ALPHA_VANTAGE_API_KEY`.
3. **Start the services**
	```sh
	docker-compose up -d
	```
4. **Access Airflow UI**
	- Open [http://localhost:8080](http://localhost:8080)
	- Login: `admin` / `admin`
5. **Trigger the DAG**
	- DAG name: `stock_data_pipeline`
	- You can trigger manually or wait for the scheduled run (hourly by default).

## Database Schema
The main table is `stock_prices` with columns for symbol, prices, volume, trading date, and more. See `initdb/init.sql` for details.

## Checking the Data
To view the data in Postgres:
```sh
docker-compose exec postgres psql -U airflow -d stock_data -c "SELECT * FROM stock_prices LIMIT 10;"
```

## Troubleshooting
- Ensure the Fernet and secret keys are identical for all Airflow services (see `docker-compose.yml`).
- Check Airflow logs in the UI for task errors.
- If the webserver is not accessible, check logs with:
  ```sh
  docker-compose logs airflow-webserver --tail=100
  ```
- If no data appears in the table, check your API key and network access.

## Requirements
- Docker, Docker Compose
- Alpha Vantage API key (free from https://www.alphavantage.co/support/#api-key)

## Credits
- Assignment by Devesh Kumar Arya
- Based on Airflow, Postgres, and Alpha Vantage API


## Working Screenshots 
<img width="1792" height="1120" alt="DAG" src="https://github.com/user-attachments/assets/cd45e645-8830-47fa-88c6-0d5a3477c3e8" />
<img width="1251" height="112" alt="table_data" src="https://github.com/user-attachments/assets/c6c45b67-a5e1-443e-a085-f68101eb0326" />
<img width="1792" height="668" alt="DAG1" src="https://github.com/user-attachments/assets/1ad1901b-2579-4bc0-a408-17d1c87768ef" />


