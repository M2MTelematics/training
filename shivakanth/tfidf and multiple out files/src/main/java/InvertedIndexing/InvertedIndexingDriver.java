package InvertedIndexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexingDriver extends Configured implements Tool {
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new InvertedIndexingDriver(), args);
	}
	
	public int run(String args[]) throws Exception {
		
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(InvertedIndexingDriver.class);
		job.setMapperClass(InvertedIndexingMapper.class);
		job.setCombinerClass(InvertedIndexingCombiner.class);
		job.setReducerClass(InvertedIndexingReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
		return 0;
		
	}

}
