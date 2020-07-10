package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class Sql_15_DATE_DATASET_API {

	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/biglog.txt");
				
		dataset.createOrReplaceTempView("logging_table");
		
//		Dataset<Row> results = spark.sql
	//	  ("select level, date_format(datetime,'MMMM') as month, count(1) as total " + 
		//   "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");			
		
		//DATASET --> JAVA API ::: ABOVE WAS  SQL API 
		
				dataset = dataset.select(col("level"),
						                 date_format(col("datetime"), "MMMM").alias("month"), 
						                 date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
				
				dataset = dataset.groupBy(col("level"),col("month"),col("monthnum")).count();
				dataset = dataset.orderBy(col("monthnum"), col("level"));
				dataset = dataset.drop(col("monthnum"));
		
dataset.show(100);
			
		spark.close();
	}
}
