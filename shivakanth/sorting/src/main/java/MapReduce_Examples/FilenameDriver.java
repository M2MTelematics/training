package MapReduce_Examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FilenameDriver extends Configured implements Tool{

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new FilenameDriver(), args);
	}
	
	public int run(String args[]) throws Exception {
		
		

		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(FilenameDriver.class);
		job.setPartitionerClass(FullnamePartitoner.class);
		job.setGroupingComparatorClass(FullnameGroupComparator.class);
		job.setMapperClass(FullnameMapper.class);
		job.setReducerClass(FullnameReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Fullname.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
		
	}

	
	
}
