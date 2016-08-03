package TFIDF_RT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import InvertedIndexing.InvertedIndexingCombiner;
import InvertedIndexing.InvertedIndexingDriver;
import InvertedIndexing.InvertedIndexingMapper;
import InvertedIndexing.InvertedIndexingReducer;
import TermFrequency.*;



public class Tfidf_Retake_Driver extends Configured implements Tool{
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new Tfidf_Retake_Driver(), args);
	}
	
	public int run(String args[]) throws Exception {
		
		Configuration conf = getConf();
		
		FileSystem fs = FileSystem.get(new Configuration());
	
		Path inputpath = new Path(args[0]);

		FileStatus[] fileStatusListIterator = fs.listStatus(inputpath);
		
		conf.setInt("NF", fileStatusListIterator.length);
//Term frequency job set-up
		
		Job TF = Job.getInstance(getConf(),"tf");
		
		TF.setJarByClass(TermFrequencyDriver.class);
		TF.setMapperClass(TermFrequencyMapper.class);
		TF.setCombinerClass(TermFrequencyReducer.class);
		TF.setReducerClass(TermFrequencyReducer.class);
		
		TF.setInputFormatClass(TextInputFormat.class);
		TF.setMapOutputKeyClass(Text.class);
		TF.setMapOutputValueClass(LongWritable.class);
		
		TF.setOutputKeyClass(Text.class);
		TF.setOutputValueClass(LongWritable.class);
		
		TF.setNumReduceTasks(2);
		FileInputFormat.addInputPath(TF, new Path(args[0]));
		FileOutputFormat.setOutputPath(TF, new Path(args[1]));
		
		TF.waitForCompletion(true);
		
// Inverted Indexing job set-up
		Job job = Job.getInstance(getConf(),"idf");
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
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
	
		job.waitForCompletion(true);
		
//TFIDF job set-up
		
		Job tfidf = Job.getInstance(conf,"tfidf");
		
		tfidf.setJarByClass(Tfidf_Retake_Driver.class);
		tfidf.setMapperClass(Tfidf_Retake_Mapper.class);
		tfidf.setReducerClass(Tfidf_Retake_Reducer.class);
		tfidf.setPartitionerClass(Tfidf_Retake_Partitioner.class);
		tfidf.setGroupingComparatorClass(Tfidf_Retake_Comparator.class);
		
		tfidf.setInputFormatClass(KeyValueTextInputFormat.class);
		tfidf.setMapOutputKeyClass(Tfidf.class);
		tfidf.setMapOutputValueClass(Tfidf.class);
		
		tfidf.setOutputKeyClass(Text.class);
		tfidf.setOutputValueClass(DoubleWritable.class);
		
		tfidf.setNumReduceTasks(2);
		FileInputFormat.setInputPaths(tfidf, new Path(args[1]),new Path(args[2]));
		FileOutputFormat.setOutputPath(tfidf, new Path(args[3]));
	
		tfidf.waitForCompletion(true);
		
		return 0;
		
		
}

}