package com.xg.hadoop.training.custom.input.format;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiMapper extends Mapper<Text,IntWritable,Text,IntWritable> {

	@Override
	protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		if( null != key){
			context.write(key, value);
		}
	}

}
