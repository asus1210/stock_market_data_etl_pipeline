# Stock Market Data ETL Pipeline

## Overview

This project involves an ETL (Extract, Transform, Load) pipeline for processing historical stock market data. The data is extracted from CSV files, transformed using the Polars library, and then loaded into a PostgreSQL database using SQLAlchemy. This pipeline efficiently handles large datasets and is designed to be scalable and flexible.

## Project Presentation

![ETL Pipeline](https://github.com/asus1210/stock_market_data_etl_pipeline/blob/main/project%20image.png)

## Project Structure

- **Data Source:** Historical stock market data downloaded from [Kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset).  
  This dataset contains historical daily prices for all tickers currently trading on NASDAQ.  
  It contains prices up to April 1, 2020.
  
- **Data Files:** The data consists of multiple CSV files representing daily prices of Nasdaq-traded stocks and ETFs.

## Workflow

1. **Extract Data:** 
   - CSV files containing stock data are extracted from the local directory.
   - The data is read into memory using the Polars library, which is optimized for performance with large datasets.
   - **Raw Data Structure:**
     - `Date`: Specifies trading date
     - `Open`: Opening price
     - `High`: Maximum price during the day
     - `Low`: Minimum price during the day
     - `Close`: Close price adjusted for splits
     - `Adj Close`: Adjusted close price adjusted for both dividends and splits
     - `Volume`: The number of shares that changed hands during a given day

2. **Transform Data:** 
   - The data is cleaned and transformed using Polars.
   - **Key Transformations:**
     - Extracted Year from Dates
     - Grouped the data by Year
     - Obtained Statistical Summary (mean, min, max, std, 25%, 50%, 75%) per year
   - **Transformed Data Structure:**
     ```
     root
     |-- statistic: string (nullable = true)
     |-- Open: double (nullable = true)
     |-- High: double (nullable = true)
     |-- Low: double (nullable = true)
     |-- Close: double (nullable = true)
     |-- Adj Close: double (nullable = true)
     |-- Volume: double (nullable = true)
     |-- Year: integer (nullable = true)
     |-- symbol: string (nullable = true)
     ```

3. **Load Data:** 
   - The transformed data is loaded into a PostgreSQL database using SQLAlchemy.
   - **Database Schema:**
     - Designed to support efficient queries on stock data.
     - Partitioned table created to store data efficiently.
     - Partitioning based on Year Range.

## Technologies Used

- **Polars:** A fast DataFrame library designed to handle large datasets with low memory usage.
- **SQLAlchemy:** A Python SQL toolkit and Object-Relational Mapping (ORM) library.
- **PostgreSQL:** An advanced, open-source relational database.

## Key Features

- **Performance:** The use of Polars for data transformation ensures fast processing even with large datasets.
- **Scalability:** The pipeline can easily scale to handle more data or additional transformation steps.
- **Database Integration:** Leveraging SQLAlchemy and PostgreSQL for robust data storage and retrieval.

## Conclusion

This ETL pipeline demonstrates an efficient approach to processing large datasets, with a focus on performance and scalability. It's an ideal solution for applications that require regular ingestion and transformation of stock market data.
