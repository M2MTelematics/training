package com.xg.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
	
	
private LongWritable outputCount = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum=0l;
		for(IntWritable val : values){
			sum = sum + val.get();
		}
		outputCount.set(sum);
		context.write(key, outputCount);
		
	}

}
