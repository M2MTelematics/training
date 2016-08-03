package TFIDF_RT;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Tfidf_Retake_Mapper extends Mapper<Text, Text, Tfidf, Tfidf>{

	private Tfidf word = new Tfidf();
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Tfidf, Tfidf>.Context context)
			throws IOException, InterruptedException {

    
		String [] Splits = key.toString().split(":");
		
		word.setWord(new Text(Splits[0]));
		word.setFilename(new Text(Splits[1]));
		word.setCnt(new LongWritable(Long.parseLong(value.toString())));
		
		context.write(word, word);
		
		
	}

	
	
}
