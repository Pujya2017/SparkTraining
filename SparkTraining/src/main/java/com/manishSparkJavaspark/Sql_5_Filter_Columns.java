package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Sql_5_Filter_Columns {
	
public static void main(String args[]) throws InterruptedException {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
	//RDD Method to Ceate Spark Contetx
	//	SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	//	JavaSparkContext sc = new JavaSparkContext(conf);
		
//****************************//
///SPARK SQL We Need SPARK SESSION Obj *********** ??????////
		
SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();


Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");


Column subjectColumn= dataset.col("subject"); ///column import from org.apache.spark.sql

//NOw We can Use This Object in Expression

Dataset<Row> filtered= dataset.filter(subjectColumn.equalTo("Modern Art"));

filtered.show();

spark.close();

}

}
