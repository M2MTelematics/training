package mapreduce_prctice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

public Text outpkey = new Text();
public IntWritable one= new IntWritable(1); 
	@Override
	public void map(LongWritable inpkey, Text inpvalue,Context context)
			throws IOException, InterruptedException {
		String inputline = inpvalue.toString();
		String [] splitting_value = inputline.split("\\W+");
		
		for(String words:splitting_value)
		{
			if(words.length()>0)
			{
				outpkey.set(words);
				context.write(outpkey,one);
			}
		}
		
	}
		
}