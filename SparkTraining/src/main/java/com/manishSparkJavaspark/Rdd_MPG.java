package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class Rdd_MPG {
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

		// Extract Total MPG from auto string with reduce class
		class totalMPG implements Function2<String, String, String> {

			@Override
			public String call(String arg0, String arg1) throws Exception {
				
				//Initialize 
				int firstVal = 0;
				int secondVal = 0;

				// First parameter - might be a numeric or string. handle appropriately
				firstVal = (isNumeric(arg0) ? 
		            Integer.valueOf(arg0) : getMPGValue(arg0));
				// Second parameter.
				secondVal = (isNumeric(arg1) ? 
		            Integer.valueOf(arg1) : getMPGValue(arg1));

				return Integer.valueOf(firstVal + secondVal).toString();
			}

			// Internal function to extract MPG
			private int getMPGValue(String str) {
				// System.out.println(str);
				String[] attList = str.split(",");
				if (isNumeric(attList[9])) {
					return Integer.valueOf(attList[9]);
				} else {
					return 0;
				}
			}

			// Internal function to check if value is numeric
			private boolean isNumeric(String s) {
				return s.matches("[-+]?\\d*\\.?\\d+");
			}

		}

		// Use external function to compute Average MPG
				String totMPG = autoData.reduce(new totalMPG());
				System.out.println("Average MPG is " + 
						(Integer.valueOf(totMPG) / (autoData.count() )));
}

}
