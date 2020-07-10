package com.manishSparkJavaspark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class RDD_CarTypes {

    public static void main(String args[]) throws InterruptedException {


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("Day3_demo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> myCSVRdd = sc.textFile("target/My_Data/auto-data.csv");

        String header = myCSVRdd.first();

        JavaRDD<String> myCSVRdd_data = myCSVRdd.filter(s -> !s.equals(header));

        JavaPairRDD<String, Integer> pairRDD =  myCSVRdd_data.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] rec = s.split(",");
                String car_type = rec[4];
                return new Tuple2(car_type, 1);
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                return v1._1.equals("hatchback")||v1._1.equals("sedan");
            }
        }).reduceByKey((a, b) -> (a+b));

        //System.out.println(pairRDD);
        pairRDD.foreach(value -> System.out.println(value));
        List<List<Integer>> input_data = new ArrayList<>();
    }
}