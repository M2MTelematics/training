package com.xg.hadoop.training.custom.input.format;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xg.hadoop.training.WordCountDriver;
import com.xg.hadoop.training.WordCountMapper;
import com.xg.hadoop.training.WordCountReducer;

public class WikiDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(WikiDriver.class);
		
		job.setMapperClass(WordCountMapper.class);
		
		job.setInputFormatClass(WikiInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(0);
				
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new WikiDriver(), args);
		
	}

}
