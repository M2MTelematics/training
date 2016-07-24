package com.xg.hadoop.training.distributed.cache;

import java.net.URI;

import javax.print.attribute.standard.NumberOfInterveningJobs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedCacheDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new DistributedCacheDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf(), "Distrbuted Cache");
		job.setJarByClass(DistributedCacheDriver.class);
		
		job.addCacheFile(new URI(args[2]));
		
		job.setMapperClass(DistributedCacheMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
		
		return 0;
	}

}
