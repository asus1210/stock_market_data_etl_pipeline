"""Stock Statistics per year"""

from pathlib import Path
import polars as pl
from sqlalchemy import create_engine, URL
from sqlalchemy.exc import SQLAlchemyError

# constants
DATA_DIR = Path(r".\data\stocks")
COPY_SQL_QUERY = """COPY public.stocks_statistics_partitioned (statistic, open_price, high,
                    low, close_price, adj_close,
                    volume, stock_year, symbol)
                    FROM stdin WITH CSV HEADER DELIMITER ',';"""
url = URL.create(
    drivername="postgresql+psycopg2",
    username="postgres",
    password="$Asus#1210",
    host="localhost",
    port=5432,
    database="stock_market"
)

try:
    engine = create_engine(url)
    # Step 2: Establish a raw connection using psycopg2
    connection = engine.raw_connection()
    print("Connection Done..")
except SQLAlchemyError as sql_err:
    print("SQL Error", sql_err)


def get_stock_summary(file_path: Path, output_file_path:Path) -> None:
    """return summary of stock per year"""
    df = pl.read_csv(file_path, try_parse_dates=True)  # read csv file and parse dates
    # extract year from date column save in date
    df = df.with_columns(pl.col("Date").dt.year().alias("Date"))
    summary_df_list = []
    # iterate through every group
    # get statistical summary
    for date, group in df.group_by("Date"):
        group_vals = group.select(
            pl.col(["Open", "High", "Low", "Close", "Adj Close", "Volume"])
        ).describe()
        group_vals = group_vals.filter(
            ~pl.col("statistic").is_in(["count", "null_count"])
        )  # remove rows with these ["count", "null_count"] values
        # Add which year the summary belong to
        group_vals = group_vals.with_columns(pl.lit(date[0]).alias("Year"))
        group_vals = group_vals.with_columns(pl.lit(file_path.stem).alias("symbol"))
        # store all the dataframes
        summary_df_list.append(group_vals)
    # create single dataframe
    df_stock_statistics_per_year = pl.concat(summary_df_list)
    df_stock_statistics_per_year.write_csv(output_file_path)
    print('Done : From', file_path.name," To cleaned", output_file_path.name)


def load_data_into_db(file_path: Path) -> None:
    """bulk insert data into database"""
    try:
        # Step 3: Use the connection to get a psycopg2 cursor
        cursor = connection.cursor()

        # Step 5: Open the CSV file and execute the COPY command using copy_expert
        with open(file=file_path, mode="r", encoding="utf-8") as file:
            cursor.copy_expert(COPY_SQL_QUERY, file)

        # Step 6: Commit the transaction
        connection.commit()
        print("Done success", file_path.name)

    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        connection.rollback()  # Roll back the transaction in case of error

    finally:
        cursor.close()

def main():
    """main"""
    new_dir = Path("./data/cleaned_stocks")
    new_dir.mkdir(parents=True, exist_ok=True)
    try:
        for file_path in DATA_DIR.iterdir():
            output_path = new_dir/file_path.name
            get_stock_summary(file_path, output_file_path=output_path)
            load_data_into_db(output_path)
    except FileNotFoundError as file_err:
        print("File Error", file_err)
    finally:
        connection.close()

if __name__ == "__main__":
    main()
