package MapReduce_Examples;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FullnameGroupComparator extends WritableComparator{

	public FullnameGroupComparator(){
		super(Fullname.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		
		Fullname p1 = (Fullname) a;
		Fullname p2 = (Fullname) b;
		
		return p1.getLname().compareTo(p2.getLname());
		
	}
	
	

}
