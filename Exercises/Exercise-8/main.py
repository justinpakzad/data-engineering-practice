import duckdb


def table_schema():
    return {
        "VIN (1-10)": "VARCHAR",
        "County": "VARCHAR",
        "City": "VARCHAR",
        "State": "VARCHAR",
        "Postal Code": "INTEGER",
        "Model Year": "INTEGER",
        "Make": "VARCHAR",
        "Model": "VARCHAR",
        "Electric Vehicle Type": "VARCHAR",
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "VARCHAR",
        "Electric Range": "INTEGER",
        "Base MSRP": "INTEGER",
        "Legislative District": "INTEGER",
        "DOL Vehicle ID": "BIGINT",
        "Vehicle Location": "VARCHAR",
        "Electric Utility": "VARCHAR",
        "2020 Census Tract": "BIGINT",
    }


def create_table_from_csv(table_schema, csv_path):
    duckdb.sql(
        f"""
        CREATE TABLE e_vehicle_population AS 
        SELECT * FROM read_csv('{csv_path}',columns={table_schema})
        """
    )
    for column in table_schema.keys():
        updated_column = (
            column.replace("(1-10)", "number").strip().replace(" ", "_").lower()
        )
        duckdb.sql(
            f"""
            ALTER TABLE e_vehicle_population 
            RENAME "{column}" TO "{updated_column}" 
            """
        )


def number_of_cars_per_city():
    duckdb.sql(
        """
        SELECT CITY,COUNT(DISTINCT vin_number) as n_cars 
        FROM e_vehicle_population GROUP BY 1"""
    ).show()


def top_3_most_popular():
    duckdb.sql(
        """
        SELECT
            model
        FROM(
        SELECT 
            model,
            DENSE_RANK() OVER(ORDER BY COUNT(distinct vin_number) DESC) AS rnk
        FROM e_vehicle_population
        GROUP BY model
        )
        WHERE rnk <= 3
        """
    ).show()


def most_popular_per_postcode():
    duckdb.sql(
        """
        SELECT
           *
        FROM(
        SELECT
            model,
            postal_code,
            COUNT(*) cnt,
            ROW_NUMBER() OVER(PARTITION BY postal_code ORDER BY COUNT(distinct vin_number) DESC) AS rn
        FROM e_vehicle_population
        GROUP BY model,postal_code)
        WHERE rn = 1
       
    """
    ).show()


def number_of_cars_per_year():
    duckdb.sql(
        """
        COPY
        (SELECT 
            model_year,
            COUNT(distinct vin_number) 
        FROM e_vehicle_population 
        GROUP BY model_year)
        TO 'n_cars_per_year'
        (FORMAT 'parquet',PARTITION_BY(model_year),OVERWRITE_OR_IGNORE) 
        """
    )


def main():
    tble_schema = table_schema()
    create_table_from_csv(
        table_schema=tble_schema, csv_path="data/Electric_Vehicle_Population_Data.csv"
    )

    number_of_cars_per_city()
    top_3_most_popular()
    most_popular_per_postcode()
    number_of_cars_per_year()


if __name__ == "__main__":
    main()
