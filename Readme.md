# README: ETL Script for CSV to PostgreSQL

## Overview
This project is an ETL (Extract, Transform, Load) script designed to ingest data from CSV files into a PostgreSQL database. The script connects to a PostgreSQL instance using credentials stored in an environment file (.env), processes CSV data using pandas, and executes queries to interact with the database.

## Features

Flexible Environment Configuration: Uses .env to securely manage database connection parameters.
Data Ingestion: Reads multiple CSV files and prepares the data for ingestion into PostgreSQL.
Dynamic Column Retrieval: Automatically retrieves column names from CSV files for table schema definition.
Database Querying: Executes SQL queries to interact with PostgreSQL tables.

## Prerequisites

### Software Requirements
- Python 3.8+
- PostgreSQL database
  
### Required Python libraries:
- os
- psycopg2
- pandas
- python-dotenv
  
### Environment Configuration
   
Create a .env file in the project directory with the following format (also available as .env(template) .txt:

```bash
DB_HOST=your_database_host
DB_USER=your_database_username
DB_PASSWORD=your_database_password
DB_NAME=your_database_name
DB_PORT=your_database_port
```

Replace the placeholders with your actual database credentials.

## Installation
-  Clone the Repository
```bash
git clone <repository_url>
cd <repository_name>
```

- Set Up Environment
Create a .env file as described in the Prerequisites section.

## Script Structure
### Library Imports
The script begins by importing necessary libraries:

- os: For reading environment variables.
- psycopg2: For PostgreSQL database interaction.
- dotenv: For loading environment variables from the .env file.
- pandas: For CSV data manipulation.

### Database Connection
The script establishes a connection to the PostgreSQL database using credentials from the .env file. It ensures error handling for connection and cursor initialization.

### CSV File Loading
Four CSV files (movies.csv, links.csv, ratings.csv, tags.csv) are loaded into pandas dataframes.

### Dynamic Table Column Extraction
The get_tables function dynamically extracts column names from dataframes and stores them as a dictionary. This can be used to construct table schemas.

### SQL Query Execution
A sample query is executed to retrieve data from the ratings table. The results are loaded into a pandas dataframe for further processing.

### Disconnect
The script closes the connection and prints a success message upon completion.

## Usage
- Place your CSV files in the tables directory.
- Run the script using:
```bash
python script_name.py
```
- Verify the results in the PostgreSQL database or view the output in the terminal.
## Function Descriptions
#### get_tables(**kwargs)
- Description: Extracts column names from dataframes.
- Arguments:Keyword arguments where the key is the table name, and the value is the corresponding dataframe.
- Returns: A list of dictionaries containing table names as keys and column names as values.
##### Example Usage:
```python
table_columns = get_tables(movie=movies, link=links, rating=ratings, tag=tags)
```
Sample Output
The script will output:

- Dataframe samples for the SQL query executed.
- Logs indicating the connection status and success messages.

## Error Handling
- Database Connection: Captures and logs errors if the script cannot connect to the database.
- SQL Execution: Handles errors during SQL query execution.

## Future Improvements
- Automated Table Creation: Extend the script to dynamically create PostgreSQL tables based on the extracted column schema.
- Data Validation: Add validation to check CSV data integrity before ingestion.
- Command-line Arguments: Allow users to specify input files and table names via CLI.
- Batch Processing: Handle large CSV files using chunk-based ingestion.

