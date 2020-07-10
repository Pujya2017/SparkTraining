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

public class Sql_2_MaxScore_Avg {
	public static void main(String args[]) throws InterruptedException {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		///SPARK SQL We Need SPARK SESSION Object *********** ?????? ////
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir","file:///c:/tmp")
				.getOrCreate();
		
		//spark.read ---> See All Options-- read csv, json, txt, jdbc etc...
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");
		
		dataset.show();
		
		long num_of_Rows=dataset.count();
		System.out.println("There are " + num_of_Rows + " rows");
		
		dataset.createOrReplaceTempView("my_students_table");
	      Dataset<Row> sqlMaxScorebySubj=spark.sql("select subject,max(score) max_score from my_students_table group by subject order by max_score desc");
	        System.out.println("Max Score by Subject Row Count:" + sqlMaxScorebySubj.count());
	        sqlMaxScorebySubj.show();

	        Dataset<Row> sqlSubjAvgMarks=spark.sql("select subject,count(student_id),avg(score) avg_score from my_students_table group by subject ");
	        System.out.println("Count of Students & Avg Score by Subject Row Count:" + sqlSubjAvgMarks.count());
	        sqlSubjAvgMarks.show();
			
		spark.close();	
	}
}
