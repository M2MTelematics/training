package MapReduce_Examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Employee implements WritableComparable<Employee> {
	
	private Text UID = new Text();
	private Text filname = new Text();
		
	public Employee(){
		UID = new Text();
		filname = new Text();
		
	}


	public Text getFilname() {
		return filname;
	}

	public void setFilname(Text filname) {
		this.filname = filname;
	}

	public Text getUID() {
		return UID;
	}

	public void setUID(Text uID) {
		UID = uID;
	}


	public int compareTo(Employee o) {
		// TODO Auto-generated method stub
		
		int cmp = this.UID.compareTo(o.UID);
	
		if( cmp == 0){
			
			cmp = this.filname.compareTo(o.filname);
			//if(this.filname.equals("us_states.csv")) cmp=1;
		}
		
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.UID.readFields(in);
		this.filname.readFields(in);
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.UID.write(out);
		this.filname.write(out);
		
	}
	
	

}
