## ETL Pipeline v1

### Introduction
Building an ETL pipeline that carries out the following tasks:

- Extracts data of +400K invoices from Redshift


- Transforms the data by:

  - Joining two tables
  - Removing all Nulls
  - Removing corrupted stock codes
  - Creating new columns
  - Identifying and removing duplicates
  

- Loads the data to s3 bucket

### Requirements
The minimum requirements:

- Docker for Mac: [Docker >= 20.10.2](https://docs.docker.com/docker-for-mac/install/)
- Docker for Windows: [Docker](https://docs.docker.com/desktop/install/windows-install/)
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


