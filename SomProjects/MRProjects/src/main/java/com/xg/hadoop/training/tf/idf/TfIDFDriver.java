package com.xg.hadoop.training.tf.idf;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TfIDFDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job1 = Job.getInstance(getConf(), "tf");
		job1.setJarByClass(TfIDFDriver.class);
		//set related job properties
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		Job job2 = Job.getInstance(getConf(), "df");
		job2.setJarByClass(TfIDFDriver.class);
		//set df related job properties
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		job2.waitForCompletion(true);
		
		Job job3 = Job.getInstance(getConf(), "tf_df");
		job3.setJarByClass(TfIDFDriver.class);
		//set tf_df related job properties
		FileInputFormat.setInputPaths(job3, new Path(args[1],new Path(args[2])));
		FileOutputFormat.setOutputPath(job1, new Path(args[3]));
		job3.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new TfIDFDriver(), args);
	}

}
