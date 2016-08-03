package TFIDF_RT;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Tfidf implements WritableComparable<Tfidf> {

	private Text word = new Text();
	private Text filename = new Text();
	private LongWritable cnt = new LongWritable();
	
	public LongWritable getCnt() {
		return cnt;
	}

	public void setCnt(LongWritable cnt) {
		this.cnt = cnt;
	}

	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word = word;
	}

	public Text getFilename() {
		return filename;
	}

	public void setFilename(Text filename) {
		this.filename = filename;
	}

	
	
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.word.readFields(in);
		this.filename.readFields(in);
		this.cnt.readFields(in);
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.word.write(out);
		this.filename.write(out);
		this.cnt.write(out);
		
	}

	public int compareTo(Tfidf o) {
		// TODO Auto-generated method stub
		int cmp = this.word.compareTo(o.word);
		
		if(cmp == 0) 
			cmp=this.filename.compareTo(o.filename);
		
		return cmp;
	}
	
}
