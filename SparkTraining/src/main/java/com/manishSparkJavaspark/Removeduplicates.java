package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
public class Removeduplicates {
    public static void main(String args[]) throws InterruptedException {
        List<Integer> input_data=new ArrayList<>();
        input_data.add(20);
        input_data.add(21);
        input_data.add(22);
        input_data.add(22);
        input_data.add(22);
        input_data.add(25);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> myRdd = sc.parallelize(input_data);
        myRdd.collect().forEach(System.out::println);
        System.out.print("--------------------------------------------"+'\n');
        //Removing duplicates
        JavaRDD<Integer> myRdd1= myRdd.distinct();
        myRdd1.collect().forEach(System.out::println);
        System.out.print(myRdd1);
        sc.close();
    }

}

