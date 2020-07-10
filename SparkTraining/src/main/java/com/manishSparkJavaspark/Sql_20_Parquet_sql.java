package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Sql_20_Parquet_sql {
	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		Dataset<Row> empDf = spark.read().parquet("target/My_Data/baby_names.parquet");
		empDf.show();
		empDf.printSchema();
	
		empDf.createOrReplaceTempView("my_students_table");
	        Dataset<Row> sqlprob=spark.sql("select name,prob from my_students_table where prob=='0.081541'");
	        System.out.println("prob equals to 0.081541:" + sqlprob.count());
	        sqlprob.show();
		
	}

}
