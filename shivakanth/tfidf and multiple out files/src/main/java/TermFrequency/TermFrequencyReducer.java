package TermFrequency;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TermFrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	private LongWritable OutputCount=new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

		long sum = 0;
		for(LongWritable val:values)
		{
			sum=sum+val.get();
			
		}
		OutputCount.set(sum);
		context.write(key,OutputCount);
	}

}
