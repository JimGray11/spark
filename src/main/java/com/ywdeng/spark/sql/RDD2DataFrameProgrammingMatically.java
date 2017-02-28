package com.ywdeng.spark.sql;
/*
 *在程序运行时动态生成DataFrme的Schema类型
 *1.首先需要将原始RDD<String>转换为RDD<ROW>的形式
 *2.构造DataFrame对应的结构属性
 *3.将结构属性加入到structType中
 *4.将DataFrame 注册为零时表
 *5.对表进行操作
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class RDD2DataFrameProgrammingMatically {
   public static void main(String[] args) {
	SparkConf conf=new SparkConf()
			.setAppName("matically")
			.setMaster("local");
	JavaSparkContext sc=new JavaSparkContext(conf);
	SQLContext sql=new SQLContext(sc);
	
	JavaRDD<String> st=sc.textFile("C:\\Users\\JimG\\Desktop\\class.txt");
	//将rdd<String> 封装为RDD<ROW>
	JavaRDD<Row> stRow=st.map(new Function<String, Row>() {

		private static final long serialVersionUID = 1L;

		public Row call(String line) throws Exception {
			String[] meta=line.split(",");
			return RowFactory.create(Integer.valueOf(meta[0]),meta[1],Integer.valueOf(meta[2]));
		}
	});
	//动态构造元数据,并将每个属性Field存放到一个List中，因为StructType需要接收一个Field列表
	List<StructField> fields=new ArrayList<StructField>();
	fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
	fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
	fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
	//注意StructField和StructType都是DataTypes的子类
	StructType structType=DataTypes.createStructType(fields);
	//生成DataFrame
	DataFrame dataFrame=sql.createDataFrame(stRow, structType);
	//将生成的DataFrame注册为临时表
	dataFrame.registerTempTable("student");
	
	DataFrame df=sql.sql("select * from student where age>18");
	//将DataFrame 转换为RDD<Row>的形式
	JavaRDD<Row> rdd=df.javaRDD();
	
	df.printSchema();
	
	rdd.foreach(new VoidFunction<Row>() {
	
		private static final long serialVersionUID = 1L;

		public void call(Row t) throws Exception {
			System.out.println(t.getInt(0)+"\t"+t.getAs("name")+"\t"+t.getInt(2));
		}
	});
	
	sc.close();	
}
}
