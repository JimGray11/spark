package com.ywdeng.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * spark中提供了累加器，可以确保多个task对一个变量的进行累加操作
 * @author JimG
 *
 */
public class AccumulatorAvariable {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				  .setAppName("BroadCast")
				  .setMaster("local");
		  JavaSparkContext  sc=new JavaSparkContext(conf);
		  List<Integer> list =Arrays.asList(3,5,8,9,129,89);
		  JavaRDD<Integer> rdd=sc.parallelize(list);
		  //创建的accumulator变量，需要使用JavaSparkContext的accumulator方法
		 final Accumulator<Integer> sum=sc.accumulator(0);
		 rdd.foreach(new VoidFunction<Integer>() {
		
			private static final long serialVersionUID = 1L;

			public void call(Integer arg0) throws Exception {
				//在函数内部使用Accumulator的add方法进行累加
			  sum.add(arg0);
			}
		});
		System.out.println("最终的计算结果为："+sum.value());
		sc.close();  
	}

}
