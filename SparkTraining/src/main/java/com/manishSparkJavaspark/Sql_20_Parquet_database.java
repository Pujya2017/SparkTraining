package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Sql_20_Parquet_database {
	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		Dataset<Row> empDf = spark.read().parquet("target/My_Data/baby_names.parquet");
		empDf.show();
		empDf.printSchema();
	
		//Do data frame queries
				System.out.println("SELECT Demo :");
				empDf.select(col("name"),col("prob")).show();
		
	}

}
