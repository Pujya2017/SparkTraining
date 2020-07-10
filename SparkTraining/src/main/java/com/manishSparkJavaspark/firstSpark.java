package com.manishSparkJavaspark;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//import javax.xml.bind.SchemaOutputResolver;

public class firstSpark {


    public static void main(String args[]) throws InterruptedException {

        List<Integer> input_data=new ArrayList<>();
        input_data.add(20);

        input_data.add(21);
        input_data.add(22);
        input_data.add(23);
        input_data.add(24);
        input_data.add(25);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);



        JavaRDD<Integer> myRdd = sc.parallelize(input_data);
        myRdd.collect().forEach(System.out::println);

        JavaRDD<String> readFile = sc.textFile("C:\\Users\\Admin\\Desktop\\myspark\\SparkTraining\\target\\input_Data\\testdata.txt");
        System.out.println("Count is " + readFile.count());
        //SqrtRdd.foreach(value->System.out.println(value));
        //New Way of Foreach in Java 1.8 is
        //SqrtRdd.foreach(System.out::println);

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




