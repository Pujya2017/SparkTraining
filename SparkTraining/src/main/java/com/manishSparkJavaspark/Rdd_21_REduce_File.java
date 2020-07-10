package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;


public class Rdd_21_REduce_File {
	public static void main(String args[]) throws InterruptedException {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Create a RDD from a file
		JavaRDD<String> autoAllData = sc.textFile("target/My_Data/auto-data.csv");
		System.out.println("Total Records in Autodata :" + autoAllData.count());
		System.out.println("Spark Operations : Load from CSV");
		
		//Remove first header line
			String header = autoAllData.first();
			JavaRDD<String> autoData = autoAllData.filter(s -> !s.equals(header));
			
			//Find shortest line in autoData with lambda function
			String shortest
				= autoData.reduce(new Function2<String, String, String>() {
					public String call(String x, String y) {
						return (x.length() < y.length() ? x :y);
					}
				});
				
				System.out.println("The shortest string is " + shortest);
				
}
}
