## ETL Pipeline v1

### Introduction
Building an ETL pipeline that carries out the following tasks:
- used sql query to extract the data from redshift with some transformations:
- joined two tables, online_transactions and stock_description
- removed all rows where customer id was missing
- removed corrupted stock codes
- added description to the online transactions table
- replaced missing stock description with Unknown
- fixed data type
- created new columns with invoice_month, invoice_dow, invoice_dow_name
- found and dropped duplicates in the data
- loaded the data to s3 bucket
- installed docker

### Requirements
The minimum requirements:

- Docker for Mac: [Docker >= 20.10.2](https://docs.docker.com/docker-for-mac/install/)
- Docker for Windows: 
- Installation: [Docker](https://docs.docker.com/desktop/install/windows-install/)
- Manual installation steps for older WSL version: [Docker WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install-manual#step-4---download-the-linux-kernel-update-package)

### Instructions on how to execute the code
- Copy the `.env.example` file to `.env` and fill out the environment vars.

- Make sure you are executing the code from the etl folder, and you have Docker Desktop running.

- Make sure you are executing the code from the etl folder, and you have Docker Desktop running

To run it locally first build the image.

```bash
  docker image build -t etl  
```
Then run the etl job using docker

```bash
  docker run --env-file .env etl
```


