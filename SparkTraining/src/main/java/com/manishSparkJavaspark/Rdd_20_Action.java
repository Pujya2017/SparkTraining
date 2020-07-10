package com.manishSparkJavaspark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Rdd_20_Action {
	public static void main(String args[]) throws InterruptedException {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Create RDD
		List<Integer> data = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> collData = sc.parallelize(data);
		System.out.println("Data FRom RDD");
		collData.collect().forEach(System.out::println);
		System.out.print(collData);
		
		//Find sum of numbers in collData
				int collCount = collData.reduce((x, y) -> x + y);
				System.out.print("Compute sum using reduce " + collCount);
	}
}
