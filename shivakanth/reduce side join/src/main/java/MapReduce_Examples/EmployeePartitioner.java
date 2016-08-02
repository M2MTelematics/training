package MapReduce_Examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class EmployeePartitioner extends Partitioner<Employee, Text>{

	@Override
	public int getPartition(Employee key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return key.getUID().hashCode() & Integer.MAX_VALUE % numPartitions;
	}

}
