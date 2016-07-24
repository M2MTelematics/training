package com.xg.hadoop.training;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import com.xg.hadoop.training.custom.input.format.WikiMapper;

public class InputFormatTexgt {

	
	MapDriver<Text, IntWritable, Text, IntWritable> mapDriver;
	
	MapReduceDriver<Text, IntWritable, Text, IntWritable, Object, Object> mapreducerDriver;
	
	
	public void setup(){
		WikiMapper mapper = new WikiMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		
		
		
		mapreducerDriver=MapReduceDriver.newMapReduceDriver(mapper, null);
		
	}
	

}
