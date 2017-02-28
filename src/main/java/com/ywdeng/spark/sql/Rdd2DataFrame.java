package com.ywdeng.spark.sql;


import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 *将普通的RDD转换为DataFrame 
 *1.读取普通的文本
 *2.将RDD<String>转换为RDD<类>
 *3.通过SQLContext对象使用反射机制将对象转换为DataFrame 
 *4.将DataFrame转换为注册为一张表
 *5.可以对表经常常规的操作
 *6.将操作之后的结果转换为RDD[row] 
 *7.对RDD[row]中的数据进行操作，封装为对象
 * @author JimG
 *
 */
public class Rdd2DataFrame {
   public static void main(String[] args) {
	SparkConf conf=new SparkConf()
			.setAppName("Rdd2DataFrame")
			.setMaster("local");
	
    JavaSparkContext  sc=new JavaSparkContext(conf);
    SQLContext sqlContext=new SQLContext(sc);
    
    
    JavaRDD<String> rdd=sc.textFile("C:\\Users\\JimG\\Desktop\\class.txt");
    JavaRDD<Student> student=rdd.map(new Function<String, Student>() {

		private static final long serialVersionUID = 1L;

		public Student call(String line) throws Exception {
			String[] str=line.split(",");
			Student stu=new Student();
			stu.setId(Integer.parseInt(str[0].toString()));
			stu.setName(str[1]);
			stu.setAge(Integer.parseInt(str[2].toString()));
			return stu;
		}
	});
    //使用反射机制将其转换为DataFrame
    DataFrame df=sqlContext.createDataFrame(student, Student.class);
    //将DataFrame 注册成一张表
    df.registerTempTable("student");
    //现在可以针对该表进行使用SQL语句操作,得到的是DataFrme的数据
    DataFrame  result= sqlContext.sql("select * from student where age> 18");
    //将DataFrmae 转换为RDD[row]类型的数据
    JavaRDD<Row> rowResult=result.javaRDD();
    //对RDD<ROW>中的数据进行处理
    JavaRDD<Student> st=rowResult.map(new Function<Row, Student>() {

		private static final long serialVersionUID = 1L;

		public Student call(Row v1) throws Exception {
			Student s=new Student();
			s.setId(Integer.parseInt(v1.getAs("id").toString()));
			s.setName(v1.getAs("name").toString());
			s.setAge(Integer.parseInt(v1.getAs("age").toString()));
			return s;
		}
	});
    List<Student> list= st.collect();
    for(Student s:list)
	  System.out.println(s.getId()+"\t"+s.getName()+"\t"+s.getAge());
}
}
