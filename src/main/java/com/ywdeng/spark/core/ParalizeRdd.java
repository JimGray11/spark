package com.ywdeng.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ParalizeRdd {
   public static void main(String[] args) {
  SparkConf  conf=new SparkConf()
		       .setAppName("PalizeRDD")
		       .setMaster("local");
  
  JavaSparkContext sc=new JavaSparkContext(conf);
  List<Integer> list=Arrays.asList(1,2,34,12,89,21,90);
  JavaRDD<Integer> rdd=sc.parallelize(list);
  int sum=rdd.reduce(new Function2<Integer, Integer, Integer>() {

	private static final long serialVersionUID = 1L;

	public Integer call(Integer num1, Integer num2) throws Exception {
		return num1+num2 ;
	}
});
  sc.close();
  System.out.println("集合的运算和为： "+sum);
}
}
