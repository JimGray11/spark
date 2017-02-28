package com.ywdeng.spark.sql;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 读取Json格式的数据文件,并显示了常规使用
 * @author JimG
 *
 */
public class JsonDataFrame {
    public static void main(String[] args) {
		SparkConf conf=new SparkConf().setAppName("DataFrame")
				  .setMaster("local");
		JavaSparkContext  sc=new JavaSparkContext(conf);
		SQLContext  sql=new SQLContext(sc);
		//使用是sql读取JSON格式的数据,形成DataFrame形式的数据格式
		 DataFrame dl=sql.read().json("C:\\Users\\JimG\\Desktop\\json.txt");
		 dl.show();
		 //查出某列数据
		 dl.select("name");
		 //查出几列的数据,并对其中的几列进行运算
		 dl.select(dl.col("age").plus(1),dl.col("address")).show();
		 dl.groupBy(dl.col("age")).count().show();
		 //对某列中的数据进行过滤
		 dl.filter(dl.col("age").gt(18)).show();
		 sc.close();
	}
}
