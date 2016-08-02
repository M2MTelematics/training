package MapReduce_Examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FullnamePartitoner extends Partitioner<Fullname, Text>{

	@Override
	public int getPartition(Fullname key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		
		
		return key.getLname().hashCode() & Integer.MAX_VALUE % numPartitions;
	}

	
	
}
