// Add a Column PASS/FAIL, and mark pass for grade A+, A, B+, B in the dataset
package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class Sql_25_UDF_pass {
	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// Start Spark Session or get an open Spark Session
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		//Define UDF  using LAMBDA EXPRESSION
		spark.udf().register("hasPassed", (String grade)-> (grade.equals("A+") 
				|| grade.equals("A") || grade.equals("B+") 
				|| grade.equals("B")),DataTypes.BooleanType);
		//Read data from students.csv
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");
		//Call udf created above
		dataset = dataset.withColumn("pass", callUDF("hasPassed",col("grade")) );
		//Display the dataset data post new column add with values
		dataset.show();
	}
}
