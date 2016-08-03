package TFIDF_RT;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Tfidf_Retake_Reducer extends Reducer<Tfidf, Tfidf, Text, DoubleWritable>{

	//private Tfidf browse = new Tfidf();
	private long TF = 0;
	private long IDF = 0;
	//private Text outkey = new Text();
	private DoubleWritable TFIDF=new DoubleWritable();
	int nf = 0;
	@Override
	protected void reduce(Tfidf key, Iterable<Tfidf> values, Reducer<Tfidf, Tfidf, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {

		nf = context.getConfiguration().getInt("NF",-1);
		for(Tfidf browse:values) {
			
			if ( browse.getFilename().toString().equals("1") ) TF=browse.getCnt().get();
			else { 
				IDF=browse.getCnt().get();
				TFIDF.set(IDF*(Math.log10(nf)/TF));
			    context.write(new Text(browse.getWord().toString()+":"+browse.getFilename().toString()+":"+nf+":"+TF+":"+IDF), TFIDF);
			      }
			
		}
		
		
	}


	
	
	
}
