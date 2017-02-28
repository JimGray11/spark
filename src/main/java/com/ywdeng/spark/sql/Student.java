package com.ywdeng.spark.sql;

import java.io.Serializable;
/**
 * 实例类必须要实现序列化接口,在sparkSql中将使用反射机制转换为DataFrame 
 * 使用反射机制的前提是必须知道原始数据的类型
 * @author JimG
 *
 */
public class Student implements Serializable {
	//我们知道或者需要在原始数据中提取该Student中是三个属性
     private int id;
     private  String name;
     private int age;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
   
}
