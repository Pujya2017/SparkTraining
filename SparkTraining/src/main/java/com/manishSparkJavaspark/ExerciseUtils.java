package com.manishSparkJavaspark;

import org.apache.spark.api.java.JavaRDD;

public class ExerciseUtils {
	
public static void hold() {
	while (true) {
		try {
			Thread.sleep(1000);
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
	}
}

public static void printStringRDD(JavaRDD<String> StringRDD, int count) {
	for (String s : StringRDD.take(count)) {
		System.out.println(s);
	}
	System.out.println("-----------------------------------------------------------------------------");
}
}
