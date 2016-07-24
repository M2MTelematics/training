package com.xg.hadoop.training.secondarysorting;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PersonPartitioner extends Partitioner<Person,Text> {

	@Override
	public int getPartition(Person key, Text value, int numReducer) {
		
		return (key.getlName().hashCode() & Integer.MAX_VALUE) % numReducer;
	}

}
