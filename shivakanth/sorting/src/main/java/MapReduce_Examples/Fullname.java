package MapReduce_Examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Fullname implements WritableComparable<Fullname> {

	private Text lname = new Text();
	private Text fname = new Text();
	
	
	
	public Text getLname() {
		return lname;
	}

	public void setLname(Text lname) {
		this.lname = lname;
	}

	public Text getFname() {
		return fname;
	}

	public void setFname(Text fname) {
		this.fname = fname;
	}

	public int compareTo(Fullname o) {
		// TODO Auto-generated method stub
		int cmp = this.getLname().compareTo(o.lname);
	
		if( cmp == 0 )
		{
			cmp=this.getFname().compareTo(o.fname);
		}
		
		return cmp;
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.lname.readFields(in);
		this.fname.readFields(in);
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.lname.write(out);
		this.fname.write(out);
	}

	
}
