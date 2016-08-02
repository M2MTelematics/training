package MapReduce_Examples;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class EmployeeMapper extends Mapper<LongWritable, Text, Employee, Text>{

	 Employee emp= new Employee();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Employee, Text>.Context context)
			throws IOException, InterruptedException {
		
		 		 
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		String[] cols = value.toString().split(",(?=([^\"]|\"[^\"]*\")*$)");
		
		if(cols.length == 12) {
		emp.setUID(new Text(cols[6]));
		emp.setFilname(new Text(fileName));
		
				}
		else { 
			emp.setUID(new Text(cols[0]));
			//emp.setFilname(new Text(fileName));
			emp.setFilname(new Text("1"));
			
		}
		context.write(emp,value);
		
		
	}

		
}
