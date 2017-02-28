package com.ywdeng.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SeconedSort {
	public static void main(String[] args) {
		//二次排序需要将需要排序的字段 封装为自定义排序的key类
		  SparkConf conf= new SparkConf()
				  .setAppName("SecondSort")
				  .setMaster("local");
		  JavaSparkContext sc=new JavaSparkContext(conf);
		  JavaRDD<String>str=sc.textFile("C:\\Users\\JimG\\Desktop\\sort.txt");
		  //将str RDD 进行封装为自定的key方式
		  JavaPairRDD<SortedKey, String> strKey=str.mapToPair(new PairFunction<String, SortedKey,String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<SortedKey, String> call(String line) throws Exception {
				String[]key=line.split("\t");
				SortedKey sortedKey=new SortedKey(Integer.parseInt(key[0]), Integer.parseInt(key[1]));
			
				return new Tuple2<SortedKey, String>(sortedKey, line);
			}
		});
		  
		   //对新生成了key-value的键值对调用sortedByKey的方法，按照key进行排序
		   JavaPairRDD<SortedKey, String> sortedStrKey=strKey.sortByKey();
		  sortedStrKey.foreach(new VoidFunction<Tuple2<SortedKey,String>>() {

			private static final long serialVersionUID = 1L;

			public void call(Tuple2<SortedKey, String> t) throws Exception {
				
			   System.out.println(t._2);
				
			}
		});	
		  sc.close();
	}
	
   
}
