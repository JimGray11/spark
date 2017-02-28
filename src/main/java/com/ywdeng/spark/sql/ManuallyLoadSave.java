package com.ywdeng.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * 在读取文件的时候手动指定输入文件类型和输出文件类型,直接可以作为DataFrame操作的常见的两种
 * 操作文件是Json和parquet
 * 或者是由RDD<String>转换为DataFrame 
 * @author JimG
 *
 */
public class ManuallyLoadSave {
 @SuppressWarnings("deprecation")
public static void main(String[] args) {
	 SparkConf conf = new SparkConf() 
				.setMaster("local")
				.setAppName("GenericLoadSave");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		//和RDD2DataFrame不同的是,此处直接由sparkSQL来创建DataFrame
		DataFrame df=sqlContext.read().format("json").load("C:\\Users\\JimG\\Desktop\\people.json");
		df.printSchema();
		df.show();
		df.select("name","age").save("C:\\Users\\JimG\\Desktop\\people.parquet","parquet",SaveMode.Overwrite);
}
}
