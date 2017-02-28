package com.ywdeng.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;


public class BroadCastVariable {
	
	public static void main(String[] args) {
		
		SparkConf conf=new SparkConf()
				  .setAppName("BroadCast")
				  .setMaster("local");
		  JavaSparkContext  sc=new JavaSparkContext(conf);
		  List<Integer> list =Arrays.asList(3,5,8,9,129,89);
		  JavaRDD<Integer> rdd=sc.parallelize(list);
		  //申请一个变量，在默认情况下是为每一个task拷贝一份
		  int num=100;
		  //如果使用广播变量则为每个节点拷贝一个，这样减少了网络传输和内存消耗,将变量设置为广播变量
		 final Broadcast<Integer> b=sc.broadcast(num);
		  
		  JavaRDD<Integer> rs=rdd.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;
            int a =b.getValue();                
			public Integer call(Integer arg) throws Exception {
				return arg*a;
			}
			//对于cache()和persist()的调用是有规则的，必须在RDD transformation 
			//或者是textFile操作之后，直接调用。单独调用是无效的
		}).persist(StorageLevel.MEMORY_ONLY());
		  
		  //我们希望把计算好新RDD持久化到内存中,cache 是默认持久化到内存中的
		  //将新的处理结果进行输出
		  rs.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			public void call(Integer arg) throws Exception {
			 System.out.print(arg+"\t");
			}
		});
		  
	}
  
  
}
