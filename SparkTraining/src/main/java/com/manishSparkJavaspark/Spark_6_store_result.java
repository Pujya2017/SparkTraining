package com.manishSparkJavaspark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark_6_store_result {
	
	public static void main(String args[]) throws InterruptedException {
		List<Integer> input_data=new ArrayList<>();
		input_data.add(20);
		
		input_data.add(21);
		input_data.add(22);
		input_data.add(23);
		input_data.add(24);
		input_data.add(25);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> myRdd = sc.parallelize(input_data);
		System.out.println("Total Records in RDD is :" + myRdd.count());
		
		//Storing RDDs
			//Make sure the file does not exist.
			myRdd.saveAsTextFile("C:\\Users\\Admin\\Desktop\\myspark\\SparkTraining\\target\\My_Data\\Output-Data.csv");
			
			//Print All data
			
			myRdd.collect().forEach(System.out::println);
			System.out.println(myRdd);
		
	}

}
