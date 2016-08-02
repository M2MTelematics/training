package MapReduce_Examples;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmployeeReducer extends Reducer<Employee, Text, Text, Text>{

	private Text outval = new Text();
	
	
	
		@Override
	protected void reduce(Employee key, Iterable<Text> values, Reducer<Employee, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
			String Location="";
			String temp="";
			String[] cols = {};
		    outval.clear();
		
		for(Text val:values)
		{
			
			cols = val.toString().split(",(?=([^\"]|\"[^\"]*\")*$)");
			if(cols.length == 2) Location =cols[1];
			else {
						
			if(Location.equals("")) {
			 cols[6]="NO LOCATION FOUND";	
			} else {
				cols[6] = Location;
			}
							
		}
			temp=temp+":"+Arrays.toString(cols);
			outval.set(temp);
         
	}
		
			
		context.write(key.getUID(),outval);
	}

	
}
