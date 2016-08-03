package TermFrequency;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TermFrequencyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

	private Text Keyout = new Text();
	private LongWritable Valueout= new LongWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    String [] Splits = value.toString().split("\\W+");

    for(String word:Splits)
    {
    	if(word.length()>0)
    	{
    		Keyout.set(word+':'+fileName);
    		context.write(Keyout,Valueout);
    	}
    }
    }
}


