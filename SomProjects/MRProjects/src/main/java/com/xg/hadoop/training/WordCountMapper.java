package com.xg.hadoop.training;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class WordCountMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
	
	public enum WordCountMapperCounter{
		TOTAL_VOWEL_RECORDS,
		TOTAL_CONSONENT_RECORDS,
		TOTAL_LINES_PROCESSED,
		TOTAL_EMPTY_LINES
	}
	private Text outputKey = new Text();
	private IntWritable one = new IntWritable(1);
	private String clientWord;
	
	String[] vowels = {"A","E","I","O","U"};
	List<String> vList = Arrays.asList(vowels);
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		clientWord = context.getConfiguration().get("WORD");
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.getCounter(WordCountMapperCounter.TOTAL_LINES_PROCESSED).increment(1);
		
		if(value.toString().isEmpty()){
			context.getCounter(WordCountMapperCounter.TOTAL_EMPTY_LINES).increment(1);
		}
				
		String [] splits = value.toString().split("\\W+");
		for(String word:splits){
			if(word.length() > 0){
				
				String firstChar = word.substring(0, 1);
				
				if(vList.contains(firstChar.toUpperCase())){
					context.getCounter(WordCountMapperCounter.TOTAL_VOWEL_RECORDS).increment(1);
				}
				else{
					context.getCounter(WordCountMapperCounter.TOTAL_CONSONENT_RECORDS).increment(1);
				}
				
				outputKey.set(word);
				context.write(outputKey, one);
				
			}
		}
	}

}
