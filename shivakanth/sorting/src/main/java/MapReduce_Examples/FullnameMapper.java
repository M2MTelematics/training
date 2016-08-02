package MapReduce_Examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FullnameMapper extends Mapper<Text, Text, Fullname, Text>{

	Fullname opkey = new Fullname();
	
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Fullname, Text>.Context context)
			throws IOException, InterruptedException {
		
		opkey.setLname(key);
		opkey.setFname(value);

	context.write(opkey, value);
	}
	
	
}
