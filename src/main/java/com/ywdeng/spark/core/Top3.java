package com.ywdeng.spark.core;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * topn排序主要是为了取出RDD中的前三条数据记录
 * @author JimG
 */
public class Top3 {
  public static void main(String[] args) {
		//二次排序需要将需要排序的字段 封装为自定义排序的key类
	  SparkConf conf= new SparkConf()
			  .setAppName("SecondSort")
			  .setMaster("local");
	  JavaSparkContext sc=new JavaSparkContext(conf);
	  JavaRDD<String>str=sc.textFile("C:\\Users\\JimG\\Desktop\\top.txt");
	  //实现方法还是需要将排序字段设置为key值，之后根据key进行排序，最后使用
	  //take取出前三行
	  JavaPairRDD<String, String> strKey=str.mapToPair(new PairFunction<String, String, String>() {

		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(String line) throws Exception {
			return new Tuple2<String, String>(line, line);
		}
	});
	  JavaPairRDD<String, String> sortedKey=strKey.sortByKey(false);
	   Iterator<Tuple2<String,String>> take=sortedKey.take(3).iterator();
	   while(take.hasNext()){
		  System.out.println(take.next()._2);
	   }
	  
	  
	  sc.close();
}
}
