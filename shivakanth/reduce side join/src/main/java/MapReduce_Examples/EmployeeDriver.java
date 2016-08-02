package MapReduce_Examples;

import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmployeeDriver extends Configured implements Tool{
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new EmployeeDriver(), args);
	}
	
	public int run(String args[]) throws Exception {
		
		

		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(EmployeeDriver.class);
		job.setPartitionerClass(EmployeePartitioner.class);
		job.setGroupingComparatorClass(EmployeeGroupComparator.class);
		job.setMapperClass(EmployeeMapper.class);
		job.setReducerClass(EmployeeReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Employee.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setOutputValueClass(Iterable.class);
		
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
		
	}


}
