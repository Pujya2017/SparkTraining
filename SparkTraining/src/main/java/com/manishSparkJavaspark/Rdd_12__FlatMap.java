package com.manishSparkJavaspark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;

public class Rdd_12__FlatMap {
	public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Create a RDD from a file
		JavaRDD<String> autoAllData = sc.textFile("C:\\Users\\Admin\\Desktop\\myspark\\SparkTraining\\target\\My_Data\\auto-data.csv");
		System.out.println("Total record Count in file: " + autoAllData.count());
		System.out.println("Spark Operations : Load from CSV ");
		
		JavaRDD<String> words = autoAllData.flatMap(new FlatMapFunction<String, String>() {
			public Iterator <String> call(String s) {
				return Arrays.asList(s.split(",")).iterator();
			}
		});
		
		System.out.println("Words Count: " + words.count());
		ExerciseUtils.printStringRDD(words, 10);
		
	}

}
