package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Rdd_KV_Method {
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


		
// KV Pair Class to make pairs
class getKV implements PairFunction<String, String, Integer[]> {

	@Override
	public Tuple2<String, Integer[]> call(String arg0) throws Exception {

		String[] attList = arg0.split(",");
		// Handle header line
		Integer[] hpVal = { (attList[7].equals("HP") ?
            0 : Integer.valueOf(attList[7])), 1 };
		return new Tuple2<String, Integer[]>(attList[0], hpVal);
	}

}

// Class to compute Average HP by make.
class computeTotalHP implements 
        Function2<Integer[], Integer[], Integer[]> {

	@Override
	public Integer[] call(Integer[] arg0, Integer[] arg1) 
                throws Exception {

		Integer[] retval = { arg0[0] + arg1[0], arg0[1] + arg1[1] };
		return retval;
	}

}

		/*-------------------------------------------------------------------
		 * Key Value / Pair RDDs
		 -------------------------------------------------------------------*/
		// Create a KV RDD with auto Brand and Horse Power.

		JavaPairRDD<String, Integer[]> autoKV 
                = autoData.mapToPair(new getKV());

		System.out.println("KV RDD Demo - raw tuples :");
		for (Tuple2<String, Integer[]> kvList : autoKV.take(5)) {
			System.out.println(kvList._1 + " - " 
					+ kvList._2[0] + " ,  " + kvList._2[1]);
		}

		// Compute Average HP by each key
		// Find summarize total HP and total Count
		JavaPairRDD<String, Integer[]> autoSumKV 
			= autoKV.reduceByKey(new computeTotalHP());

		System.out.println("KV RDD Demo - Tuples after summarizing :");
		for (Tuple2<String, Integer[]> kvList : autoSumKV.take(5)) {
			System.out.println(kvList._1 + " - " + 
					kvList._2[0] + " ,  " + kvList._2[1]);
		}

		// Now find average
		JavaPairRDD<String, Integer> autoAvgKV 
			= autoSumKV.mapValues(x -> x[0] / x[1]);

		System.out.println("KV RDD Demo - Tuples after averaging :");
		for (Tuple2<String, Integer> kvList : autoAvgKV.take(5)) {
			System.out.println(kvList);
		}
		
		//******************

}
}
