package com.ywdeng.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
/**
 * 数据源为Json格式的学生信息和成绩信息两个数据源
 * 实现查询成绩为80分以上的学生的基本信息与成绩信息
 * @author JimG
 */

public class JsonDataSource {
 public static void main(String[] args) {
	SparkConf conf=new SparkConf()
			.setMaster("local")
			.setAppName("JsonDataSource");
	JavaSparkContext  sc=new JavaSparkContext(conf);
	SQLContext sql=new SQLContext(sc);
	//学生的分数信息来源json格式的文件
	DataFrame scores=sql.read().json("C:\\Users\\JimG\\Desktop\\score.txt");
	//学生的基本信息来自于自己的list集合
	List<String> infos=new ArrayList<String>();
	infos.add("{\"name\":\"Jobs\",\"age\":20}");
	infos.add("{\"name\":\"Tom\",\"age\":19}");
	infos.add("{\"name\":\"Machinel\",\"age\":21}");
	JavaRDD<String> info=sc.parallelize(infos);
	//将infos形式的数据源转换为DataFrame，再转换为RDD<Row>的形式来读取数据
	/**
	 * 注意在使用json格式作为数据源的时候，会自动推断列的属性
	 */
	DataFrame infoDf=sql.read().json(info);
	//将scores注册为一张临时表
	scores.registerTempTable("scores");
	DataFrame resultScores=sql.sql("select * from scores where score >80");
	//将查找结果注册为一张临时表
	resultScores.registerTempTable("sc");
	 //从成绩大于80的学生中选择出，学生的名字，用于从学生信息表中查找学生的对应信息
	List<String> name=resultScores.javaRDD().map(new Function<Row, String>() {

		private static final long serialVersionUID = 1L;

		public String call(Row line) throws Exception {
			return line.getString(0);
		}
	}).collect();
	
	
	/*	实现方式一、将DataFrame 转化为RDD<Row>之后，在转换为RDD<key,value>键值对的形式,最后使用jion操作
	JavaPairRDD<String,Integer> s=resultScores.javaRDD().mapToPair(new PairFunction<Row,String, Integer>() {

		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> call(Row t) throws Exception {
			return new Tuple2<String, Integer>(t.getString(0),Integer.valueOf(String.valueOf(t.getLong(1))));
		}
	});*/
	
	
	/**
	 * infoDf.printSchema();
	 * 注意：在读取Json数据源的时候，会重新对排序，所以在知道属性名的情况下建议
	 * 使用getAs()而尽量少使用getString()这种形式，通常情况下会出现，类型转换异常
	 */

 /*JavaPairRDD<String, String> in=infoDf.javaRDD().mapToPair(new PairFunction<Row, String, String>() {

		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(Row t) throws Exception {
			
			return new Tuple2<String, String>(t.getAs("name").toString(),t.getAs("age").toString());
		}
	});
	*/
	/**
	 * 实现方式二、将DataFrame注册为一张表，并使用sparkSQL 来进行处理
	 */
	infoDf.registerTempTable("info");
	//编写需要执行sql语句
    String  sqlStr="select * from info where name in(";
    //使用循环实现对sql语句的拼接,特别需要注意SQL的拼写
   		 for(int i=0;i<name.size();i++){
             sqlStr +="'"+name.get(i)+"'"+",";
             if(i==name.size()-1)
            	 sqlStr+="'"+name.get(i)+"'"+")";
   		 }
   //对两张表进行join连接操作
   		 String sql2="select a.name ,a.age,b.score from info a "
   		 		+ "left join sc b on a.name=b.name";
	     DataFrame stDataFrame=sql.sql(sql2);
	
	//对两张表进行join操作
	stDataFrame.javaRDD().foreach(new VoidFunction<Row>() {
	
		private static final long serialVersionUID = 1L;

		public void call(Row t) throws Exception {
	     System.out.println(t.getString(0)+"\t"+t.getLong(1)+"\t"+t.getLong(2));
		}
	});;
	
	
	/**
	 * 将两个数据源进行join操作，并通过RowFacotry将结果转换为RDD<ROW>的形式
	 */
	 /* JavaRDD<Row> result=s.join(in).map(new Function<Tuple2<String,Tuple2<Integer,String>>, Row>() {

		private static final long serialVersionUID = 1L;

		public Row call(Tuple2<String, Tuple2<Integer, String>> tup) throws Exception {
			return RowFactory.create(tup._1,tup._2._2,tup._2._1);
		}
	} );
*/
/*	result.foreach(new VoidFunction<Tuple2<String,Tuple2<Long,String>>>() {

		private static final long serialVersionUID = 1L;

		public void call(Tuple2<String, Tuple2<Long,String>> r) throws Exception {
			System.out.println(r._1+"\t"+r._2._1+"\t"+r._2._2);
			
		}
	});*/
	
/** 需要将数据结果保存为新的DataFrame形式：
	 * 1.通过反射机制
	 * 2.通过动态生成元数据的方式*/
	/* 
	ArrayList<StructField>list=new ArrayList<StructField>();
	list.add(DataTypes.createStructField("name", DataTypes.StringType, true));
	list.add(DataTypes.createStructField("age", DataTypes.StringType, true));
	list.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
	StructType type=DataTypes.createStructType(list);
	
	DataFrame sudent=sql.createDataFrame(result, type);
	*//**
	 *将结果保存为Json的格式
	 *//*
	sudent.write().json("C:\\Users\\JimG\\Desktop\\st.json")*/;
	
	
	sc.close();
}
}
