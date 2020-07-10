package com.manishSparkJavaspark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



public class Sql_36_Shuffle_partition {

	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
		spark.conf().set("spark.sql.shuffle.partitions", 8);
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/biglog.txt");
		
	dataset.createOrReplaceTempView("logging_table");
		
		Dataset<Row> results = spark.sql
	  ("select level, date_format(datetime,'MMMM') as month, count(1) as total, date_format(datetime,'M') as monthnum " +    "from logging_table group by level, month, monthnum order by monthnum, level");			
		

		results.show(100);
		
	results.explain();
		
	Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
			
		spark.close();
	}

}

/// Have a Look of Spark UI for this JOB  Uncomment Scanner 2 Line
