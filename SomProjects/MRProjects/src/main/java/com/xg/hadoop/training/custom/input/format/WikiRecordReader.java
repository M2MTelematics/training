package com.xg.hadoop.training.custom.input.format;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class WikiRecordReader extends RecordReader<Text, IntWritable>{
	
	private Text key;
	private IntWritable value;
	LineRecordReader lineReader;

	
	 public WikiRecordReader() {
		lineReader = new LineRecordReader();
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null != key? key:null;
	}

	@Override
	public IntWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null != value?value:null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lineReader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		lineReader.initialize(arg0, arg1);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if(!lineReader.nextKeyValue())
			return false;
		
			Text line = lineReader.getCurrentValue();
			
			String[] splits = line.toString().split(" ");
			
			if(splits.length > 3){
				String projectName = splits[0];
				String pageName = splits[1];
				String pageCount = splits[2];
				
				if(projectName.equals("en")){
					key = new Text(pageName);
					value = new IntWritable(Integer.parseInt(pageCount));
					
				}
				
			}
			return true;
	}

}
