def write_to_postgres(df, table_name, pg_config, mode="append"):
    (
        df.write
            .format("jdbc")
            .option("url", pg_config["url"])
            .option("dbtable", table_name)
            .option("user", pg_config["user"])
            .option("password", pg_config["password"])
            .option("driver", pg_config["driver"])
            .mode(mode)
            .save()
    )
