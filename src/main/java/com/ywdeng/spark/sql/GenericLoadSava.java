package com.ywdeng.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class GenericLoadSava {
     /**
      * 对于 sparkSQL无论从读取什么的数据源来创建DataFrame 都有共同的操作
      * load 和save操作
      */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf() 
				.setMaster("local")
				.setAppName("GenericLoadSave");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		//和RDD2DataFrame不同的是,此处直接由sparkSQL来创建DataFrame
		DataFrame df=sqlContext.read().load("C:\\Users\\JimG\\Desktop\\users.parquet");
		//可以对DataFrema直接进行操作
		df.printSchema();
		
       df.select("name","favorite_color","favorite_numbers").write().save("C:\\Users\\JimG\\Desktop\\us.parquet");
		
	}
	
}
