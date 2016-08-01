package mapreduce_prctice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WordCountReducer extends Reducer<Text,IntWritable,Text, IntWritable>{
	
	@Override
	public void reduce(Text key,Iterable<IntWritable> listOfValuesfrommapper, Context context) throws IOException,InterruptedException
	{
	 int sum=0;
	 for(IntWritable values:listOfValuesfrommapper)
	 {
		 sum = sum + values.get();
	 }
	 context.write(key,new IntWritable(sum));
	 }
}
	
	
	