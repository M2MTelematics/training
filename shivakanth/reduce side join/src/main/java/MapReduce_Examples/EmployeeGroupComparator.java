package MapReduce_Examples;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class EmployeeGroupComparator extends WritableComparator {

	public EmployeeGroupComparator() {
		super(Employee.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Employee e1 = (Employee) a;
		Employee e2 = (Employee) b;
		return e1.getUID().compareTo(e2.getUID());
	}
	


}
