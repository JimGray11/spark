package com.ywdeng.spark.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;


/**
 * 使用hive作为数据源
 * 1、可以建表
 * 2、把数据插入到表中，数据源可以是本地的，也可以是HDFS上的
 * 3、将处理好的数据保存hive表中
 * @author JimG
 *
 */

public class HiveDataSource {
	public static void main(String[] args) {
	SparkConf conf=new SparkConf()
			.setMaster("local")
			.setAppName("hiveDataSource");
	JavaSparkContext sc=new JavaSparkContext(conf);
	HiveContext hc=new HiveContext(sc.sc());
	//向hive中插入表 score 
	hc.sql("create table if not exists score(name:String,sc:int)");
    //将数据上传到新建表score中
	hc.sql("load data local inpath \"/home/hadoop/app/student_scores.txt\""
			+ "into table score");
	
	/**
	 * 使用同样的方法创建学生信息表Info
	 */
	hc.sql("create table if not exists info(name:String,age:int)");
    //将数据上传到新建表info中
	hc.sql("load data local inpath \"/home/hadoop/app/student_infos.txt\""
			+ "into table info");
	/*
	 *对两张表进行连接操作
	 */
    DataFrame goodStudent=hc.sql("select a.name,a.sc,b.age from a"
     		+ "left join info b on a.name=b.name"
     		+ "where a.sc>=80");
	//将最终的结果保存在hive中
    /*
     *先创建表名，之后使用SaveAsTable将数据保存到表中
     */
    hc.sql("create table if not exists student");
    //之间将dataFrame保存到表中
    goodStudent.write().saveAsTable("student");
    //直接从hive表中取出数据 ,
    List<Row> s=hc.table("student").javaRDD().collect();
    for(Row row:s)
    	System.out.println("s=>"+row.getString(0)+"\t"+
       row.getInt(1)+"\t"+row.getInt(2));
	
	sc.close();
	}
}
