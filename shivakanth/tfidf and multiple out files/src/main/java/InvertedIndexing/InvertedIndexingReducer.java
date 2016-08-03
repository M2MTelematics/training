package InvertedIndexing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.mapreduce.Reducer;


public class InvertedIndexingReducer extends Reducer<Text, Text, Text, LongWritable>{

	//private  List<Text> outnames = new ArrayList<Text>();
	//private Text outcome=new Text();
    
	
	/*
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		outnames.clear();
		
	}
	*/
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		
		List<String> list = new ArrayList<String>();
		LongWritable df = new LongWritable(0);
		
		for(Text val:values)
		{ 
			list.add(val.toString());
			
		}	
	
		
		//outcome.clear();
			
		HashSet<String> outvalues = new HashSet<String>(list);
		/*		
		for(String val:outvalues)
		{ 
			outcome.set(val.replaceAll(",,", ",")+","+outcome.toString().replaceAll(",,", ","));
			
		}
		*/
		
		df.set(outvalues.size());
		context.write(new Text(key.toString().concat(":1")),df);
	}
		
		
		
	}


