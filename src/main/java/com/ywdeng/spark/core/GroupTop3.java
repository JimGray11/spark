package com.ywdeng.spark.core;

import java.util.ArrayList;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;


import scala.Tuple2;


/**
 * 分组后的topN分析，主要是对分组后的Iterable中数据进行排序，取出同一组中的前n个数
 * @author JimG
 */

public class GroupTop3 {

  public static void main(String[] args) {
	//二次排序需要将需要排序的字段 封装为自定义排序的key类
	  SparkConf conf= new SparkConf()
			  .setAppName("SecondSort")
			  .setMaster("local");
	  JavaSparkContext sc=new JavaSparkContext(conf);
	  JavaRDD<String>str=sc.textFile("C:\\Users\\JimG\\Desktop\\score.txt");
	  JavaPairRDD<String, String> pairRDD=str.mapToPair(new PairFunction<String, String,String>() {

		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(String line) throws Exception {
			String[] st=line.split("\t");
			return new Tuple2<String, String>(st[0], st[1]);
		}
	});
	  JavaPairRDD<String, Iterable<String>> group=pairRDD.groupByKey();
	  //获取没组中前是三个数据
	  JavaPairRDD<String, Iterable<String>> top3=group.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String,Iterable<String>>() {

		private static final long serialVersionUID = 1L;

		public Tuple2<String, Iterable<String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
			 Iterator<String> it=t._2.iterator();
			  //从it中选出
			 List<Integer> list=new ArrayList();
			 int i=0;
             while(it.hasNext()&& i<3){
            	 ++i;
            	 list.add(Integer.parseInt(it.next()));
             }   
             list.sort(new Comparator<Integer>() {
 				public int compare(Integer o1, Integer o2) {
 					return o2-o1;
 				}
 			});
			
			return new Tuple2<String, Iterable<String>>(t._1,(Iterable<String>) list.iterator());
		}
	});
	 top3.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {
	
		private static final long serialVersionUID = 1L;

		public void call(Tuple2<String, Iterable<String>> t) throws Exception {
		    Iterator<String> iterator=t._2.iterator();
		    while(iterator.hasNext()){
		    	System.out.print(t._1+"\t"+iterator.next());		    	
		    }
		    
			
		}
	}); 
	  
    sc.close();	  
}
}
