package com.ywdeng.spark.core;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author JimG
 * spark 中的常用action 操作包括
 * reduce 、collect、count、take、foreach、saveAsTextFile、countByKey 
 */

public class Action {
	
 public static void main(String[] args) {
	SparkConf conf=new SparkConf()
			.setAppName("Action")
			.setMaster("local");
   JavaSparkContext  sc=new JavaSparkContext(conf);
   //调用相关操作
   // reduce(sc); 
   
   //countAndTake(sc);
   //collect(sc);
   countByKey(sc);
   sc.close();
 }
 /*
  *主要是统计Tuple2 中key出现的次数
  */
 private static void countByKey(JavaSparkContext sc) {
	 @SuppressWarnings("unchecked")
	List<Tuple2<String, String>> coreList=Arrays.asList(new Tuple2<String, String>("class1", "tom"),
			 new Tuple2<String, String>("class1", "tom"),
			 new Tuple2<String, String>("class2", "xiaoming"),
			 new Tuple2<String, String>("class1", "aihua"),
			 new Tuple2<String, String>("class2", "yalishanda"),
			 new Tuple2<String, String>("class1", "weilian"),
			 new Tuple2<String, String>("class1", "gaoerfu"));
	 JavaPairRDD<String, String> score=sc.parallelizePairs(coreList);
	 //注意Countkey返回的是Map<String,Object>
      Map<String, Object> keyCount = score.countByKey();
      for(Map.Entry<String,Object> entry: keyCount.entrySet())
    	  System.out.println(entry.getKey()+ "\t"+entry.getValue());
}
//collect 操作主要是把数据从集群取到本地
 private static void collect(JavaSparkContext sc) {
	 /**
	  * 该方法主要统计单词的个数，为查看结果把统计结果输出到控制台上
	  * 同时，保存到文件中
	  */
	JavaRDD<String> stRdd=sc.textFile("C:\\Users\\JimG\\Desktop\\exception.txt");
    JavaRDD<String> word=stRdd.flatMap(new FlatMapFunction<String,String>() {
		private static final long serialVersionUID = 1L;
		public Iterable<String> call(String line) throws Exception {
			return  Arrays.asList(line.split(" "));
		}

	});
    
    JavaPairRDD<String, Integer>pair=word.mapToPair(new PairFunction<String, String,Integer>() {

		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> call(String word) throws Exception {
			return new Tuple2<String, Integer>(word, 1);
		}
	});
    //将相同key的值进行的值进行相加
      JavaPairRDD<String, Integer>pait2plus=pair.reduceByKey(new Function2<Integer, Integer, Integer>() {

		private static final long serialVersionUID = 1L;

		public Integer call(Integer arg0, Integer arg1) throws Exception {
			return arg0+arg1;
		}
	});
      //将数据从集群上取到本地并显示在控制台上
     List<Tuple2<String, Integer>>pairLocal=pait2plus.collect();
     for(Tuple2<String, Integer> tup: pairLocal){
    	 System.out.println(tup._1+" \t"+tup._2);
     }
     pait2plus.saveAsTextFile("C:\\Users\\JimG\\Desktop\\statistic.txt");
 }
/**
  * count 主要是计算RDD中元素的个数
  * @param sc
  */
 private static void countAndTake(JavaSparkContext sc) {
	 JavaRDD<String> stRdd=sc.textFile("C:\\Users\\JimG\\Desktop\\exception.txt");
	 long lineNum=stRdd.count();
	 System.out.println("RDD 中元素个数为： "+lineNum);
	 //获取RDD中的三个元素,取回来元素存放在list中
	 List<String> newRdd=stRdd.take(10);
	 for(String str:newRdd)
		 System.out.println(str);
}
/*
  * Reduce操作
  */
private static void reduce(JavaSparkContext sc) {
  List<Integer> list = Arrays.asList(1,2,4,5,78,90);
  JavaRDD<Integer> listRdd= sc.parallelize(list);
  int sum =listRdd.reduce(new Function2<Integer, Integer, Integer>() {
	
	private static final long serialVersionUID = 1L;

	public Integer call(Integer num1, Integer num2) throws Exception {
		return num1+num2;
	}
});
  System.out.println("列表中的数计算的结果为："+sum);
}
}
