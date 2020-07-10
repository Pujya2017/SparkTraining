package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class Sql_15_Student_DATASET_API {

	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");
				
		dataset.createOrReplaceTempView("logging_table");
		
// Java API
		
				dataset = dataset.select(col("subject"),
						                 max(col("score")).alias("maxscore") );
				
				//dataset = dataset.groupBy(col("subject"),col("maxscore")).count();
		
dataset.show(100);
			
		spark.close();
	}
}
