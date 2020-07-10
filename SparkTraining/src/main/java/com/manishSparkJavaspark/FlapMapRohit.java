package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;


public class FlapMapRohit {
    public static void main(String a[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> autoAllData = sc.textFile("C:\\\\Users\\\\Admin\\\\Desktop\\\\myspark\\\\SparkTraining\\\\target\\\\My_Data\\\\auto-data.csv",2);
        System.out.println("Count is " + autoAllData.count());
        JavaRDD<String> mapWord = autoAllData.map((x)->(x));

        JavaRDD<String> flatMapword = autoAllData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        System.out.println("Word Count is ::::   "+flatMapword.count());

        mapWord.collect().forEach(System.out::println);
        flatMapword.collect().forEach(System.out::println);
        sc.close();


    }
}
