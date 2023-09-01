# Python scripts that extract the data from redshift and carries out some of the transformation tasks in sql
from typing import Iterator

import pandas as pd
import psycopg2
from pandas import DataFrame


# function to connect to redshift
def connect_to_redshift(dbname, host, port, user, password):
    """Method that connects to redshift. This gives a warning so will look for another solution"""

    connect = psycopg2.connect(
        dbname=dbname, host=host, port=port, user=user, password=password
    )

    print("connection to redshift made")

    return connect

# function to extract, join and clean transactional data
def extract_transactional_data(dbname, host, port, user, password):

    """
    This function connects to redshift and carries out the following transformation tasks
    1. Removes all rows where customer id is missing
    2. Removes the stock codes M, D, CRUK, POST, BANK CHARGES,
    3. Adds description to the online transactions table
    4. Replaces missing stock description with Unknown
    5. Fixes the data type of invoice date
    """


# connect to redshift
    connect = connect_to_redshift(dbname, host, port, user, password)

# read the sql query which carries out the above 5 tasks

    query = """
        SELECT ot.invoice, 
           ot.stock_code,
           CASE WHEN s.description IS NULL THEN 'Unknown'
                ELSE s.description END AS description,
           ot.price,
           ot.quantity,        
           CAST(invoice_date As DateTime) AS invoice_date,
           ot.customer_id,
           ot.country,
           ot.price * ot.quantity AS total_order_value
        FROM bootcamp.online_transactions ot
        /* this is a subquery that removes '?' from the stock_description table */
        LEFT JOIN (SELECT *
               FROM bootcamp.stock_description
               WHERE description <> '?') AS s ON ot.stock_code = s.stock_code
    WHERE ot.customer_id <> ''
      AND ot.stock_code NOT IN ('BANK CHARGES', 'POST', 'D', 'M', 'CRUK')
        """
    online_trans_cleaned = pd.read_sql(query, connect)

    print("The shape of the extracted and transformed data is", online_trans_cleaned.shape)

    return online_trans_cleaned

# Task 1 - Can you create an invoice_month variable with the name of the month?

def extract_transactional_data_month(dbname, host, port, user, password):
    connect = connect_to_redshift(dbname, host, port, user, password)

    query = '''SELECT ot.invoice, ot.stock_code, ot.quantity, ot.price, ot.customer_id, ot.country,
                case when sd.description is null then 'Unknown' 
                    else description end as description,
                    CAST(ot.invoice_date AS datetime) as invoice_date,
    CASE 
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 1 THEN 'January'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 2 THEN 'February'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 3 THEN 'March'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 4 THEN 'April'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 5 THEN 'May'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 6 THEN 'June'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 7 THEN 'July'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 8 THEN 'August'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 9 THEN 'September'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 10 THEN 'October'
        WHEN EXTRACT(MONTH FROM CAST (ot.invoice_date as datetime)) = 11 THEN 'November'
        ELSE 'December' END AS invoice_month
        FROM bootcamp.online_transactions ot
            LEFT JOIN (select *
                     from bootcamp.stock_description
                     where description <> '?') sd on ot.stock_code = sd.stock_code
    WHERE ot.stock_code not in ('BANK CHARGES', 'POST', 'D', 'M', 'CRUK')
      AND customer_id <> ''
            '''
    online_trans_w_description_task1 = pd.read_sql(query,connect)

    print("The shape of the extracted and transformed data is", online_trans_w_description_task1)

    return online_trans_w_description_task1

# Task2 - Can you create an invoice_dow variable that gives you the number of the day of the week?

def extract_transactional_data_dow(dbname, host, port, user, password):
    connect = connect_to_redshift(dbname, host, port, user, password)

    query = '''SELECT ot.invoice, ot.stock_code, ot.quantity, ot.price, ot.customer_id, ot.country, 
                case when sd.description is null then 'Unknown' 
                    else description end as description,
                    extract (DOW FROM CAST (ot.invoice_date as datetime)) as invoice_dow
            from bootcamp.online_transactions ot
            LEFT JOIN (select *
                     from bootcamp.stock_description
                     where description <> '?') sd on ot.stock_code = sd.stock_code
            WHERE ot.stock_code not in ('BANK CHARGES', 'POST', 'D', 'M', 'CRUK')
            AND customer_id <> '';'''

    online_trans_w_description_task2 = pd.read_sql(query,connect)

    print("The shape of the extracted and transformed data is", online_trans_w_description_task2)

    return online_trans_w_description_task2

# Task3 - Can you create an invoice_dow_name variable that gives you the name of the week? Monday is 0, tuesday is 1….
def extract_transactional_data_dow_name(dbname, host, port, user, password):
    connect = connect_to_redshift(dbname, host, port, user, password)


    query = '''SELECT ot.invoice, ot.stock_code, ot.quantity, ot.price, ot.customer_id, ot.country,ot.invoice_date,
                case when sd.description IS NULL THEN 'Unknown' 
                    else description END AS description,
                    extract (DOW FROM CAST (ot.invoice_date as datetime)) as invoice_dow,
    CASE 
        WHEN EXTRACT (DOW FROM CAST (ot.invoice_date as datetime))-1 = 0 THEN 'Monday'
        WHEN EXTRACT (DOW FROM CAST (ot.invoice_date as datetime))-1 = 1 THEN 'Tuesday'
        WHEN EXTRACT (DOW FROM CAST (ot.invoice_date as datetime))-1 = 2 THEN 'Wednesday'
        WHEN EXTRACT (DOW FROM CAST (ot.invoice_date as datetime))-1 = 3 THEN 'Thursday'
        WHEN EXTRACT (DOW FROM CAST (ot.invoice_date as datetime))-1 = 4 THEN 'Friday'
        WHEN EXTRACT (DOW FROM CAST (ot.invoice_date as datetime))-1 = 5 THEN 'Saturday'
        ELSE 'Sunday' END AS invoice_dow_name
        FROM bootcamp.online_transactions ot
            LEFT JOIN (SELECT *
                     FROM bootcamp.stock_description
                     WHERE description <> '?') sd ON ot.stock_code = sd.stock_code
    WHERE ot.stock_code NOT IN ('BANK CHARGES', 'POST', 'D', 'M', 'CRUK')
            AND customer_id <> '';'''

    online_trans_w_description_task3 = pd.read_sql(query,connect)

    print("The shape of the extracted and transformed data is", online_trans_w_description_task3)

    return online_trans_w_description_task3
