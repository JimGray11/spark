package com.ywdeng.spark.core;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;


import scala.Tuple2;


public class Transformation {
  public static void main(String[] args) {
	 SparkConf conf=new SparkConf()
			        .setAppName("Transformation")
			        .setMaster("local");
	 JavaSparkContext  sc=new JavaSparkContext(conf);
	 //进行方法的调用
	 //map(sc);
	 //filter(sc);
	 //flatMap(sc);
	 //groupByKey(sc);
	 //reduceByKey(sc);
	 //sortByKey(sc);
	 //sc关闭
	 jionAndCogroup(sc);
	 sc.close();
  }
  //对两个tuple2 类型的RDD进行连接操作
  private static void jionAndCogroup(JavaSparkContext sc) {
	  //此处需要将学生的姓名和成绩进行连接操作
	  @SuppressWarnings("unchecked")
	List<Tuple2<Integer, String>> nameList=Arrays.asList(
			  new Tuple2<Integer, String>(111,"张三"),
			  new Tuple2<Integer, String>(111,"张三丰"),
			  new Tuple2<Integer, String>(112,"李四"),
			  new Tuple2<Integer, String>(113 ,"顾恺之"),
			  new Tuple2<Integer, String>(114,"张之洞"),
			  new Tuple2<Integer, String>(115,"李鸿章"),
			  new Tuple2<Integer, String>(116,"乾隆"),
			  new Tuple2<Integer, String>(117,"康熙"));
	  @SuppressWarnings("unchecked")
	   List<Tuple2<Integer, Integer>> scoreList=Arrays.asList(
			  new Tuple2<Integer, Integer>(117,90),
			  new Tuple2<Integer, Integer>(113 ,72),
			  new Tuple2<Integer,Integer>(115,59),
			  new Tuple2<Integer, Integer>(112,89),
			  new Tuple2<Integer, Integer>(114,87),
			  new Tuple2<Integer, Integer>(111,66),
			  new Tuple2<Integer, Integer>(111,77),
			  new Tuple2<Integer,Integer>(116,98));
   //根据集合初始化RDD
	  JavaPairRDD<Integer, String> name=sc.parallelizePairs(nameList);
	  JavaPairRDD<Integer,Integer> score=sc.parallelizePairs(scoreList);
	  JavaPairRDD<Integer, Tuple2<String, Integer>> record=name.join(score);
	  record.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

		private static final long serialVersionUID = 1L;

		public void call(Tuple2<Integer, Tuple2<String, Integer>> t1) throws Exception {
		    System.out.println("学号： "+ t1._1$mcI$sp()+"  姓名："+t1._2._1+"  成绩："+t1._2._2);
		}
	});
   //使用cogroup进行连接操作
	  JavaPairRDD<Integer, Tuple2<Iterable<String>,Iterable<Integer>>> coRdd=name.cogroup(score);
	  
	  coRdd.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

		private static final long serialVersionUID = 1L;

		public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
			System.out.print("学号： "+ t._1$mcI$sp()+"  姓名：");
			Iterator<String> it=t._2._1.iterator();
			while(it.hasNext()){
			System.out.print("\t"+it.next());
			}
			System.out.print("\t"+"成绩");
			
			Iterator<Integer> it2=t._2._2.iterator();
			while(it2.hasNext()){
			System.out.print("\t"+it2.next());
			}
			System.out.println();
			
		}
	});
	  
}
/***
   * 对学生的成绩进行排序
   * @param sc
   */
  private static void sortByKey(JavaSparkContext sc) {
	  @SuppressWarnings("unchecked")
	List<Tuple2<Integer, String>> tup=Arrays.asList(
			  new Tuple2<Integer, String>(90,"张三"),
			  new Tuple2<Integer, String>(72,"李四"),
			  new Tuple2<Integer, String>(59 ,"顾恺之"),
			  new Tuple2<Integer, String>(89,"张之洞"),
			  new Tuple2<Integer, String>(87,"李鸿章"),
			  new Tuple2<Integer, String>(66,"乾隆"),
			  new Tuple2<Integer, String>(98,"康熙"));
      //根据list初始化RDD操作
	  JavaPairRDD<Integer,String> rdd=sc.parallelizePairs(tup);
	  //进行map转换
	  JavaPairRDD<Integer, String>rdd2=rdd.sortByKey(false);
	  rdd2.foreach(new VoidFunction<Tuple2<Integer,String>>() {

		private static final long serialVersionUID = 1L;

		public void call(Tuple2<Integer, String> t1) throws Exception {
			System.out.println(t1._2+"\t"+t1._1);
		}
	});
  }
/*
   * 将每个班级的成绩进行分组
   */
  @SuppressWarnings("unused")
private static void groupByKey(JavaSparkContext sc) {
  @SuppressWarnings("unchecked")
List<Tuple2<String, Integer>> tup=Arrays.asList(
		  new Tuple2<String, Integer>("C1", 90),
		  new Tuple2<String, Integer>("C1", 72),
		  new Tuple2<String, Integer>("C2", 59),
		  new Tuple2<String, Integer>("C1", 89),
		  new Tuple2<String, Integer>("C3", 87),
		  new Tuple2<String, Integer>("C2", 66),
		  new Tuple2<String, Integer>("C3", 98));
  JavaPairRDD<String, Integer> tupRdd=sc.parallelizePairs(tup);
  //返回的是Tuple2<v1,Iterable<t1,t2,t3>>的形式
  JavaPairRDD<String, Iterable<Integer>> result=tupRdd.groupByKey();
  
  result.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

	private static final long serialVersionUID = 1L;

	public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
	
	 System.out.println("class : "+t._1);
     Iterator<Integer> value=t._2.iterator();
     while(value.hasNext()){
    	System.out.print(value.next()+ "\t");
     }
     System.out.println("===========================");
	}
	
});
  
  
  
		  
		  
  
  }
/**
   * 将文本拆分为单词
   * @param sc
   */
  @SuppressWarnings("unused")
private static void flatMap(JavaSparkContext sc) {
   JavaRDD<String> text=sc.textFile("C:\\Users\\JimG\\Desktop\\spark.txt");
   JavaRDD<String> word=text.flatMap(new FlatMapFunction<String, String>() {

	private static final long serialVersionUID = 1L;

	public Iterable<String> call(String str) throws Exception {
		
		return Arrays.asList(str.split(" "));
	}
});
   word.foreach(new VoidFunction<String>() {
	
	private static final long serialVersionUID = 1L;

	public void call(String t) throws Exception {
		System.out.println(t);
	}
});
  }
/**
   *filter 函数根据自定义的表达式返回boolean值，如果是true则
   *保留该条数据记录
   * @param sc
   */
  @SuppressWarnings("unused")
private static void filter(JavaSparkContext sc) {
	//定义数据集 
     List<Integer> list=Arrays.asList(1,2,45,6,7,8,90);  
	 JavaRDD<Integer> listRdd=sc.parallelize(list);
	 JavaRDD<Integer> result=listRdd.filter(new Function<Integer, Boolean>() {

		private static final long serialVersionUID = 1L;

		public Boolean call(Integer num) throws Exception {
			return num%2==0;
		}
	});
	 result.foreach(new VoidFunction<Integer>() {
		
		private static final long serialVersionUID = 1L;

		public void call(Integer arg0) throws Exception {
			System.out.print(arg0+"\t");
		}
	});
}
/**
   * rdd中的transformation map 操作
   */
 public static void map(JavaSparkContext sc){
	 //定义数据集 
	 List<Integer> list=Arrays.asList(1,2,45,6,7,8,90);
	 
	//根据数组创建RDD数据源
	 JavaRDD<Integer> listRdd=sc.parallelize(list);
	//使用map转换将list 中的每个元素都乘以2 
	 JavaRDD<Integer>result=listRdd.map(new Function<Integer, Integer>() {

		private static final long serialVersionUID = 1L;

		public Integer call(Integer num) throws Exception {
			return num*2;
		}
	});
	 //定义一个action操作会将提交job
	 result.foreach(new VoidFunction<Integer>() {
		
		private static final long serialVersionUID = 1L;

		public void call(Integer num) throws Exception {
		   System.out.println(num);
		}
	});
	 
 }
   //rdd中filter操作  
 
}
