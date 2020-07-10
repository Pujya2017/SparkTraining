// ADD a Column PASS/FAIL in the datasets

package com.manishSparkJavaspark;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Sql_25_UDF {
	
	@SuppressWarnings("resource")
	
	
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
		
	//DEfinf UDF  using LAMBDA EXPRESSION	
		
spark.udf().register("hasPassed", (String grade)->grade.equals("A+"),DataTypes.BooleanType );

		
		
Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");
dataset = dataset.withColumn("pass", callUDF("hasPassed",col("grade")) );
		
		dataset.show();
}


}

