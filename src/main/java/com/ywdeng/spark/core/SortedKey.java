package com.ywdeng.spark.core;

import java.io.Serializable;

import scala.math.Ordered;

public class SortedKey implements Ordered<SortedKey>,Serializable {
	private static final long serialVersionUID = 1L;
	//自定义需要排序的属性,并设置属性对应的getter、setter以及hashcode方法
	int first;
	int second;
	
	public SortedKey(int first,int second){
		this.first=first;
		this.second=second;
	}
	
	public boolean $greater(SortedKey other) {
		if(this.first>other.getFirst())
			return true;
		else if(this.first==other.getFirst() && this.second>other.getSecond())
			return true;
		return false;
	}

	public boolean $greater$eq(SortedKey other) {
		if(this.$greater(other))
			return true;
		else if(this.first==other.getFirst() && this.second==other.getSecond())
			return true;
		return false;
	}

	public boolean $less(SortedKey other) {
		if(this.first<other.getFirst())
			return true;
		else if(this.first==other.getFirst() && this.second<other.getSecond())
			return true;
		return false;
	}

	public boolean $less$eq(SortedKey other) {
		if(this.$less(other))
			return true;
		else if(this.first==other.getFirst() && this.second==other.getSecond())
			return true;
		return false;
	}

	public int compare(SortedKey other) {
		if((this.first-other.getFirst())!=0)
			return this.first-other.getFirst();
		else 
			return this.second-other.getSecond();
	}

	public int compareTo(SortedKey other) {
		if((this.first-other.getFirst())!=0)
			return this.first-other.getFirst();
		else 
			return this.second-other.getSecond();
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SortedKey other = (SortedKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
   
}
