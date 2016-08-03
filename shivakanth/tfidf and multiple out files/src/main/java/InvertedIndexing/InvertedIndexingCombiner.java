package InvertedIndexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexingCombiner extends Reducer<Text, Text, Text, Text>{

	
	private Text outcome=new Text();
    	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		List<String> list = new ArrayList<String>();
		
		
		for(Text val:values)
		{ 
			list.add(val.toString());
			
		}	
	
		
		outcome.clear();
			
		HashSet<String> outvalues = new HashSet<String>(list);
			
		for(String val:outvalues)
		{ 
			outcome.set(val.replaceAll(",,", ",")+","+outcome.toString().replaceAll(",,", ","));
			
		}
		
		
		
		context.write(key,outcome);
	}
		
	
	}
