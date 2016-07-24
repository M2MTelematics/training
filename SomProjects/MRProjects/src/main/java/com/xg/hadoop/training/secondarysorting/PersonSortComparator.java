package com.xg.hadoop.training.secondarysorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PersonSortComparator extends WritableComparator {
	
	public PersonSortComparator(){
		super(Person.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		Person p1 = (Person) a;
		Person p2 = (Person) b;
		
		int cmp = p1.getlName().compareTo(p2.getlName());
		if(cmp == 0){
			cmp = p1.getfName().compareTo(p2.getfName());
			
		}
		return cmp;
	}

}
