package com.manishSparkJavaspark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import scala.Tuple2;

public class Sql1_1_Demo {
	public static void main(String args[]) throws InterruptedException {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		//RDD Method to Create Spark Context
		//SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		//JavaSparkContext sc = new JavaSparkContext(conf);
		
		//*****************************//
		///SPARK SQL We Need SPARK SESSION Object *********** ?????? ////
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp")
				.getOrCreate();
		
		//spark.read ---> See All Options-- read csv, json, txt, jdbc etc...
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");
		
		dataset.show();
		
		long num_of_Rows=dataset.count();
		System.out.println("There are " + num_of_Rows + " rows");
		
		//how to select 1st row of data
		//Row firstRow=dataset.first();
		//System.out.println("1st row is " + firstRow + " .... ");
		
		//how to select the Any column
		//String subject=firstRow.getString(2);// typecast
		//String subject=firstRow.get(2).toString();// typecast2
		//System.out.println("3rd column is " + subject + " .... ");
		//String subject1=firstRow.getAs("subject").toString();// typecast3
		//System.out.println("3rd column is " + subject1 + " .... ");
		
		//how to Make Year Column as Integer Type..
		//How to Convert Type to Integer (take Date Column and Convert to Integer
		//int year=Integer.parseInt(firstRow.getAs("year").toString());
		//System.out.println("Year is " + year );
		
		// Filter All German Subject
		//Dataset<Row> filtered=dataset.filter("subject='German'");
		//filtered.show();
		
		// Filter All German Subject & year>=2007
		//Dataset<Row> filtered1=dataset.filter("subject='Modern Art' AND year >=2007");
		//filtered1.show();
		
		//// LAMBDA EXPRESSION like RDD
		
		//Dataset<Row> modernArtResults = dataset.filter(Row->Row.getAs("subject").equals("Modern Art") && Integer.parseInt(Row.getAs("year"))>2007);
		//modernArtResults.show();
		// Above both methods are same
		
		// Using geq method
		//Dataset<Row> modernArtResults1 = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
		//modernArtResults1.show();
		
		// Create an object and use it
		//Column subjectColumn= dataset.col("subject"); ///column import from org.apache.spark.sql
		//NOw We can Use This Object in Expression
		//Dataset<Row> filtered2= dataset.filter(subjectColumn.equalTo("Modern Art"));
		//filtered2.show();
		
		// Create a subjectColumn for subject and year>= 2007
		//Column YearColumn= dataset.col("year");
		//Dataset<Row> modernArtResults2 = dataset.filter(column("subject").equalTo("Modern Art")
		//															.and(column("year").geq(2007)));
		//modernArtResults2.show();
		
		// Create a temporary table
		// Dataset<Row> modernArtResults = dataset.filter("select * from students where subject = 'Modern Art' AND year >= 2007 ");
		
				dataset.createOrReplaceTempView("my_students_table");
				
				Dataset<Row> results = spark.sql("select * from my_students_table where subject = 'Modern Art' AND year >= 2007 ");
				results.show();
				results.write().option("header", true).csv("target/My_Data/modernArtResults");// gives partitioned data
				results.coalesce(1).write().option("header", true).csv("target/My_Data/modernArtResults1");
				
			
		spark.close();	
	}
}
