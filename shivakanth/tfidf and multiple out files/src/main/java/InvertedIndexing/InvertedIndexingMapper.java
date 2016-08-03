package InvertedIndexing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexingMapper extends Mapper<LongWritable,Text,Text,Text> {

	private Text Keyout = new Text();
	private Text Valueout= new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    String [] Splits = value.toString().split("\\W+");

    for(String word:Splits)
    {
    	if(word.length()>0)
    	{
    		Keyout.set(word);
    		Valueout.set(fileName);
    		context.write(Keyout,Valueout);
    	}
    }
    }
}

