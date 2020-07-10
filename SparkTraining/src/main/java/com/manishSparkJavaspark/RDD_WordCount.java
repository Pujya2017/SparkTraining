package com.manishSparkJavaspark;

import org.apache.spark.api.java.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.Arrays;

public class RDD_WordCount {
 public static void main(String[] args) {
  Logger.getLogger("org.apache").setLevel(Level.WARN);
  SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
  JavaSparkContext sc = new JavaSparkContext(conf);

  // Load our input data.
  String inputFile = "target/My_Data/abc.txt";

  JavaRDD < String > input = sc.textFile(inputFile);
  // Split in to list of words
  JavaRDD < String > words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

  // Transform into pairs and count.
  JavaPairRDD<String,Integer> pairs = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1));

  JavaPairRDD < String, Integer > counts = pairs.reduceByKey((x, y) -> x + y);

  //System.out.println(counts.collect());
  counts.collect().forEach(System.out::println);
sc.close();
 }
}