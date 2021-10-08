""""""

from pyspark.sql import functions as F


def extract_hes_year(hes_type_regex):
	table_yyYY_col = F.regexp_extract(F.col('tableName'), "(?<=" + hes_type_regex + "_{0,1})\d{4}", 0)
	table_yy_col = F.substring(table_yyYY_col, 0, 2)
	table_year_col = (F.when(table_yy_col > 80,
							 F.concat(F.lit('19'), table_yy_col))
					  .otherwise(F.concat(F.lit('20'), table_yy_col).cast('integer'))
					  )
	return table_year_col


def get_hes_tables(hes_type_regex, database):
	"""return spark df of hes tables in database using supplied regex"""
	df_list_hes_tables = spark.sql(f"SHOW TABLES IN {database}")
	df_list_hes_tables = df_list_hes_tables.where(F.col('tableName').rlike(hes_type_regex+'(?=_{0,1}\d{4})'))

	table_path_col = F.concat(F.col('database'), F.lit("."),F.col('tableName'))

	df_list_hes_tables = df_list_hes_tables.select(table_path_col.alias('tablePath'),
												   extract_hes_year(hes_type_regex).alias('tableYear'))

	return df_list_hes_tables.orderBy(F.col('tableYear').asc())#.toPandas()['tablePath'].tolist()


def make_hes_all_years(df_hes_tables, output_table_path, partition_by_column='FYEAR', mode='replace'):
	"""appends hes tables into named output_table_name, parititioned by FYEAR"""

	if mode == 'replace':
		spark.sql(f'DROP TABLE IF EXISTS {output_table_path}')

		for hes_table in df_hes_tables.toPandas()['tablePath'].tolist():
			spark.table(hes_table).write.format('delta').mode('append').partitionBy(partition_by_column).saveAsTable(output_table_path)
	elif mode == 'update':
		df_hes_tables_sorted = df_hes_tables.orderBy(F.col('tableYear').asc()).toPandas()
		most_recent_hes_table_path = df_hes_tables_sorted.loc[-1, 'tablePath']
		most_recent_hes_year = df_hes_tables_sorted.loc[-1, 'tableYear']
		(spark.table(most_recent_hes_table_path)
				.where(F.col(partition_by_column) == most_recent_hes_year)
				.write
				.format('delta')
				.mode('overwrite')
				.option("replaceWhere", f"{partition_by_column} = '{most_recent_hes_year}'")
				.saveTableAs(output_table_path)
				)

	df_hes_all_years = spark.table(output_table_path)

	return df_hes_all_years


def update_hes_all_years(df_hes_tables, output_table_path, partition_by_column='FYEAR', replace='False'):
	"""updates just the most recent year in the hes all years table by overwriting the partition"""

	df_hes_tables_sorted = df_hes_tables.orderBy(F.col('tableYear').asc()).toPandas()
	most_recent_hes_table_path = df_hes_tables_sorted.loc[-1,'tablePath']
	most_recent_hes_year = df_hes_tables_sorted.loc[-1, 'tableYear']

	df_hes_all_years_most_recent = (spark.table(most_recent_hes_table_path)
									.where(F.col(partition_by_column) == most_recent_hes_year)
									.write
									.format('delta')
									.mode('overwrite')
									.option("replaceWhere", f"{partition_by_column} = '{most_recent_hes_year}'")
									.saveTableAs(output_table_path)
									)

	df_hes_all_years = spark.table(output_table_path)

	return df_hes_all_years