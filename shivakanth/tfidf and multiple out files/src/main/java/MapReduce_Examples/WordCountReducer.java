package MapReduce_Examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable>{

	private MultipleOutputs<Text, LongWritable> mo;
	
	private LongWritable OutputCount=new LongWritable();
	
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		mo=new MultipleOutputs<Text, LongWritable>(context);
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

		long sum = 0;
		for(IntWritable val:values)
		{
			sum=sum+val.get();
			
		}
		OutputCount.set(sum);
		
		mo.write(key, OutputCount, key.toString().substring(0, 1)+"/");
		
		//context.write(key,OutputCount);
	}


	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		mo.close();
	}

}
