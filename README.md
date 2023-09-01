## ETL Pipeline v1

### Introduction
Building an ETL pipeline that carries out the following tasks:
- use sql query to extract the data from redshift with some transformations:
- join two tables online_transactions and stock_description
- removing all rows where customer id is missing
- removing corrupted stock codes
- adding description to the online transactions table
- replacing missing stock description with Unknown
- fixing data type
- finding and dropping duplicates in the data
- loading the data to s3 bucket
- install docker

### Requirements
Python 3+
I executed this code with Python 3.11 and the libraries versions are listed in the requirements file.

## Instructions on how to execute the code
Make sure you are executing the code from the same location as the main.py script.

Install all the libraries you will need to execute main.py.
  pip3 install -r requirements.txt
Copy the .env.example file to .env and fill out the environment variables.

Run the main.py script, which carries out the extraction-transformation and load tasks.

  python3 main.py


------------------
## ETL Pipeline v2

### Introduction
Building an ETL pipeline that carries out the following tasks:
- Extracts transactional data of 400k invoices from Redshift
- Transforms the data by identifying and removing duplicates
- Loads the transformed data to an s3 bucket

### Requirements
  The minimum requirements:
- Docker for Mac: [Docker >= 20.10.2](https://docs.docker.com/docker-for-mac/install/)
- Docker for Windows: 
  - Installation: [Docker](https://docs.docker.com/desktop/install/windows-install/)
  - Manual installation steps for older WSL version: [Docker WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install-manual#step-4---download-the-linux-kernel-update-package)

### Instructions on how to execute the code
- Copy the `.env.example` file to `.env` and fill out the environment vars.

- Make sure you are executing the code from the etl_pipeline folder and you have Docker Desktop running.


To run it locally first build the image.

```bash
  docker image build -t etl-pipeline:0.1 .
```


