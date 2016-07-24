package com.xg.hadoop.training.secondarysorting;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PersonMapper extends Mapper<Text,Text,Person,Text> {
	Person opKey = new Person();
	

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Person, Text>.Context context)
			throws IOException, InterruptedException {
		opKey.setlName(key);
		opKey.setfName(value);
		
		context.write(opKey, value);
	}

}
