package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Sql_20_JSON_sql {
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
	
		empDf.createOrReplaceTempView("my_students_table");
	      Dataset<Row> sqlnamesal=spark.sql("select name,salary from my_students_table");
	        System.out.println("Max Score by Subject Row Count:" + sqlnamesal.count());
	        sqlnamesal.show();
	        
	        Dataset<Row> sqlavg40=spark.sql("select name,salary from my_students_table where age==40");
	        System.out.println("Age equals to 40:" + sqlavg40.count());
	        sqlavg40.show();

	        Dataset<Row> summaryData_sql = spark.sql("select deptid, avg(salary), max(age) from my_students_table group by deptid  ");
	        summaryData_sql.show();

	        Dataset<Row> genderData_sql = spark.sql("select gender, count(*) from employee my_students_table group by gender");
	        genderData_sql.show();
		
	}

}
