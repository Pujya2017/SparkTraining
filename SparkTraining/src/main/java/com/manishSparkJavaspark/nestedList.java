package com.manishSparkJavaspark;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class nestedList {


    public static void main(String args[]) throws InterruptedException {

        List<Integer> input_data=new ArrayList<>();
        List<Integer> input_data1=new ArrayList<>();
        List<List> nest_list = new ArrayList<>();
        input_data.add(20);

        input_data.add(21);
        input_data.add(22);
        input_data.add(23);
        input_data.add(24);
        input_data.add(25);
        input_data1.add(34);
        nest_list.add(input_data);
        nest_list.add(input_data1);
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);



        JavaRDD<Integer> myRdd = sc.parallelize(input_data);
        myRdd.collect().forEach(System.out::println);

        JavaRDD<Double> sqrtRDD = myRdd.map((value) -> (Math.sqrt(value)));

        sqrtRDD.collect().forEach(System.out::println);
        JavaRDD<List> myRdd1 = sc.parallelize(nest_list);
        myRdd1.collect().forEach(System.out::println);
        //SqrtRdd.collect().forEach(System.out::println);
        //System.out.print(result);

        //How to Count How many Elements are There in RDD
        // how many elements in sqrtRdd
        // using just map and reduce
        //JavaRDD<Long> singleIntegerRdd = SqrtRdd.map( value -> 1L);
        //Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        //System.out.println(count);

        sc.close();


    }

}




