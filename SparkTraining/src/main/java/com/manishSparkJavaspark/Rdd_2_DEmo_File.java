package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Rdd_2_DEmo_File {
	public static void main(String args[]) throws InterruptedException {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Create a RDD from a file
		JavaRDD<String> readFile = sc.textFile("C:\\Users\\Admin\\Desktop\\myspark\\SparkTraining\\target\\My_Data\\auto-data.csv");
		System.out.println("Total record Count in file: " + readFile.count());
		System.out.println("Spark Operations : Load from CSV ");
		
		sc.close();
		
	}

}
