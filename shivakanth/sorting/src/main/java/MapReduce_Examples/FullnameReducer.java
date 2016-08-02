package MapReduce_Examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FullnameReducer extends Reducer<Fullname, Text, Text, Text>{

	private Text outval = new Text();
	
	@Override
	protected void reduce(Fullname key, Iterable<Text> value, Reducer<Fullname, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		outval.clear();
		for(Text val:value)
		{
			outval.set(outval.toString()+":"+val.toString());
         
	}
		context.write(key.getLname(),outval);

	}
	
	
	
}
