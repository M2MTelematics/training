package Booksxml;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BookInputDriver extends Configured implements Tool{

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new BookInputDriver(), args);
	}
	
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
        Job job = Job.getInstance(getConf());
		job.setJarByClass(BookInputDriver.class);
		job.setMapperClass(BookinputMapper.class);
		//job.setReducerClass(WordCountReducer.class);
		
		job.setInputFormatClass(BookInput.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(IntWritable.class);
		
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
		
		
	}
	
	

}
