package com.ywdeng.spark.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.spark.sql.catalyst.expressions.ShiftRightUnsigned;
import org.stringtemplate.v4.compiler.CodeGenerator.region_return;



import scala.Tuple2;

public class JDBCDataSource {
 public static void main(String[] args) {
	 SparkConf conf = new SparkConf()
				.setAppName("JDBCDataSource");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		// 总结一下
		// jdbc数据源
		// 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
		// 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
		// 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中
		// 分别将mysql中两张表的数据加载为DataFrame
		Map<String, String> options = new HashMap<String, String>();
		options.put("url", "jdbc:mysql://spark1:3306/testdb");
		options.put("dbtable", "student_infos");
		DataFrame studentInfosDF = sqlContext.read().format("jdbc")
				.options(options).load();
		options.put("dbtable", "student_scores");
		DataFrame studentScoresDF = sqlContext.read().format("jdbc")
				.options(options).load();
		JavaPairRDD<String, String> info=studentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String,String>() {

			
			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(Row r) throws Exception {
				return new Tuple2<String, String>(r.getAs("name").toString(),r.getAs("age").toString());
		  }
		});
	    JavaPairRDD<String, List<String>> scors=studentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, List<String>>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, List<String>> call(Row t) throws Exception {
				List<String> s=new ArrayList<String>();
				s.add(t.getAs("Math").toString());
				s.add(t.getAs("EngLish").toString());
				s.add(t.getAs("History").toString());
				return new Tuple2<String, List<String>>(t.getAs("name").toString(),s );
			}
		});
	    //对两个JavaPairRDD进行join操作
	    JavaPairRDD<String, Tuple2<String, List<String>>> student=info.join(scors);
	    //将RDD转换为RDD<ROW>
        JavaRDD<Row> studentRow=student.map(new Function<Tuple2<String,Tuple2<String,List<String>>>, Row>() {
			private static final long serialVersionUID = 1L;

			public Row call(Tuple2<String, Tuple2<String, List<String>>> v1) throws Exception {
				String Math=v1._2._2.get(0);
				String EngLish=v1._2._2.get(1);
				String History=v1._2._2.get(2);
				return RowFactory.create(v1._1,v1._2._1,Math,EngLish,History);
			}
		});   
        //从中filter出成绩大于80的学生
        JavaRDD<Row>goodStudent=studentRow.filter(new Function<Row, Boolean>() {

			public Boolean call(Row v1) throws Exception {
				return Integer.valueOf(v1.getString(2))>=80;
			}
		});
	   //将数据保存到数据库MySQL数据库中
        goodStudent.foreach(new VoidFunction<Row>() {
			
			private static final long serialVersionUID = 1L;

			public void call(Row row) throws Exception {
			 String sql="insert into student values("+
			"'"+String.valueOf(row.getInt(0))+"'";
			 //注册数据库驱动
			 Class.forName("com.mysql.jdbc.Driver");
			 //获取连接
			 Connection con=null;
			 PreparedStatement statement=null;
			 try {
				con=DriverManager.getConnection("jdbc:mysql://spark1:3306/testdb", "", "");
				statement=con.prepareStatement(sql);
			} catch (Exception e) {
				// TODO: handle exception
			}finally {
				//需要手动关闭连接
				if(statement!=null)
				   statement.close();
				if(con!=null)
				 con.close();
			}
			}
			
		});
}
 }
