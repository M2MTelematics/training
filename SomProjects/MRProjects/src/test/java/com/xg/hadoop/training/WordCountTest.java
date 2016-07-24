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
import org.junit.Before;
import org.junit.Test;



import junit.framework.TestCase;

public class WordCountTest{
	MapDriver<LongWritable, Text, Text, IntWritable> wordCountMapperDriver;
	ReduceDriver<Text, IntWritable, Text, LongWritable> wordCountReducerDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, LongWritable> mapreducerDriver;
	IntWritable one = new IntWritable(1);
	

	public void setup(){
		WordCountMapper mapper = new WordCountMapper();
		wordCountMapperDriver = MapDriver.newMapDriver(mapper);
		
		WordCountReducer reducer = new WordCountReducer();
		wordCountReducerDriver = ReduceDriver.newReduceDriver(reducer);
		
		mapreducerDriver=MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
	}
	
	
	public void testMapper() throws IOException{
		wordCountMapperDriver.withInput(new LongWritable(1),new Text("I am som shekhar"));
		
		IntWritable one = new IntWritable(1);
		List<Pair<Text, IntWritable>> opList = new ArrayList();
		opList.add(new Pair<Text,IntWritable>(new Text("I"),one));
		opList.add(new Pair<Text,IntWritable>(new Text("am"),one));
		opList.add(new Pair<Text,IntWritable>(new Text("som"),one));
		opList.add(new Pair<Text,IntWritable>(new Text("shekhar"),one));
		
		wordCountMapperDriver.withAllOutput(opList);
		wordCountMapperDriver.runTest();
		
	}

	
	public void testMapper2() throws IOException{
		wordCountMapperDriver.withInput(new LongWritable(1),new Text("I am        som shekhar"));
		
		
		List<Pair<Text, IntWritable>> opList = new ArrayList();
		opList.add(new Pair<Text,IntWritable>(new Text("I"),one));
		opList.add(new Pair<Text,IntWritable>(new Text("am"),one));
		opList.add(new Pair<Text,IntWritable>(new Text("som"),one));
		opList.add(new Pair<Text,IntWritable>(new Text("shekhar"),one));
		
		wordCountMapperDriver.withAllOutput(opList);
		wordCountMapperDriver.runTest();
		
	}
	
	
	public void testMapper3() throws IOException{
		wordCountMapperDriver.withInput(new LongWritable(1),new Text("    "));
		
		
		List<Pair<Text, IntWritable>> opList = new ArrayList();
				
		wordCountMapperDriver.withAllOutput(opList);
		wordCountMapperDriver.runTest();
		
	}
	
	
	public void testReducer(){
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(one);
		values.add(one);
		wordCountReducerDriver.withInput(new Text("I"),values);
		wordCountReducerDriver.withOutput(new Text("I"),new LongWritable(2));
	}

	
	public void testReducer2(){
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(one);
		values.add(one);
		values.add(one);
		wordCountReducerDriver.withInput(new Text("I"),values);
		wordCountReducerDriver.withOutput(new Text("I"),new LongWritable(3));
	}
	
	
	public void testMainDriver() throws IOException{
		mapreducerDriver.withInput(new LongWritable(1),new Text("I am I am am som shekhar"));
		List<Pair<Text, LongWritable>> oplist  = new ArrayList<Pair<Text,LongWritable>>();
		oplist.add(new Pair<Text, LongWritable>(new Text("I"), new LongWritable(2)));
		oplist.add(new Pair<Text, LongWritable>(new Text("am"), new LongWritable(3)));
		oplist.add(new Pair<Text, LongWritable>(new Text("shekhar"), new LongWritable(1)));
		oplist.add(new Pair<Text, LongWritable>(new Text("som"), new LongWritable(1)));
		
		mapreducerDriver.withAllOutput(oplist);
		mapreducerDriver.runTest();
		
	}
};
