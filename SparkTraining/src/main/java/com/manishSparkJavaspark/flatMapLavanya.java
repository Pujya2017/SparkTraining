package com.manishSparkJavaspark;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

public class flatMapLavanya {
    public static void main(String args[]) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> autoAllData = sc.textFile("target\\test1.txt");
        /*System.out.println("Total Records in autoAllData: " + autoAllData.count());
        System.out.println("Spark Operations: Load from TEXT");*/
        JavaRDD<String> words= autoAllData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        words.collect().forEach(System.out::println);
        // System.out.println();
        sc.close();
    }
}