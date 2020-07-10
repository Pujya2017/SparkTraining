package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Sql_20_JSON_database {
	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		Dataset<Row> empDf = spark.read().json("target/My_Data/customerData.json");
		empDf.show();
		empDf.printSchema();
	
		//Do data frame queries
		System.out.println("SELECT Demo :");
		empDf.select(col("name"),col("salary")).show();
		
		System.out.println("FILTER for Age == 40 :");
		empDf.filter(col("age").equalTo(40)).show();
		
		System.out.println("GROUP BY gender and count :");
		empDf.groupBy(col("gender")).count().show();
		
		System.out.println("GROUP BY deptId and find average of salary and max of age :");
		Dataset<Row> summaryData = empDf.groupBy(col("deptid"))
			.agg(avg(empDf.col("salary")), max(empDf.col("age")));
		summaryData.show();
		
	}

}
