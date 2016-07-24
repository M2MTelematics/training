package com.xg.hadoop.training.secondarysorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PersonGroupingComparator extends WritableComparator {
	public PersonGroupingComparator(){
		super(Person.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		Person p1 = (Person) a;
		Person p2 = (Person) b;
		
		int cmp = p1.getlName().compareTo(p2.getlName());
		
		return cmp;
	}
}
