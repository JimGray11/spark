package com.ywdeng.spark.core;

import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class LineCount {
	/**
	 * RDD的transformation和action操作
	 * action: reduce 、collect 将集群中数据取到本地、count获取RDD元素总数
	 *         countByKey 对每个key对应的值进行count计算、saveAsTextFile 保存的文件
	 *         take(n)获取RDD前n个元素 ,foreach 变量RDD中的元素进行变量
	 * 常见的transformation 操作 groupByKey 、reduceByKey 、Jion、及cogroup 
	 *     同Jion操作,但是key对应的Iterable<value>都会传入自定义的函数中进行处理
	 *     sortByKey对每个key对应的value进行排序  、filter、map 等
	 */
	/**
	 * 该类中主要实现的是在文本中相同行数的统计
	 */
	public static void main(String[] args) {
	  SparkConf conf=new SparkConf()
			  .setAppName("LineCount")
			  .setMaster("local");
	  
	  JavaSparkContext sc=new JavaSparkContext(conf);
	  
	  JavaRDD<String> lines=sc.textFile("C:\\Users\\JimG\\Desktop\\spark.txt");
	  
	  JavaPairRDD<String,Integer>pair=lines.mapToPair(new PairFunction<String, String, Integer>() {

		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> call(String value) throws Exception {
			return new Tuple2<String, Integer>(value, 1);
		}
	});
	  //对新生产RDD循环进行transformation 或者是action操作
	  JavaPairRDD<String, Integer> result=pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
		
		private static final long serialVersionUID = 1L;

		public Integer call(Integer v1, Integer v2) throws Exception {
			return v1+v2;
		}
	});
	  //根据key的进行排序
	  JavaPairRDD<String, Integer> sortResult=result.sortByKey(true);
	 //使用foreach遍历RDD中的每个元素
    sortResult.foreach(new  VoidFunction<Tuple2<String,Integer>>() {

		private static final long serialVersionUID = 1L;

		public void call(Tuple2<String, Integer> t) throws Exception {
		  System.out.println(t._1+" \t"+t._2+" times");		
		}
	});	  
    sc.close();
	}

	

}
