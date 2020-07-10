package com.manishSparkJavaspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
 
public class ReadTextToRDD {
 
    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
                                        .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // provide path to input text file
        String path = "target/input_Data/testdata.txt";
        
        // read text file to RDD
        JavaRDD<String> lines = sc.textFile(path);
        
        // collect RDD for printing
        for(String line:lines.collect()){
            System.out.println(line);
        }
    }
} 