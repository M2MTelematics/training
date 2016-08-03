package MapReduce_Examples;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	private Text Keyout = new Text();
	private IntWritable Valueout= new IntWritable(1);
	private List<String> clientWord = new ArrayList<String>();
		
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException, FileNotFoundException {
		
		try{
			URI[] localpath = context.getCacheFiles();
			Path filepath = new Path(localpath[0]);
			
			//File f= new File(localpath[0]);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream iStream = fs.open(filepath);
			
			BufferedReader buffreader = new BufferedReader(new InputStreamReader(iStream));
	System.out.println("pritning file ocntents"+buffreader.readLine().toString());
	
		String words=null;
		
		while((words=buffreader.readLine()) != null){
			clientWord.add(words.toString());
		}
		buffreader.close();
					}catch(IOException e)	{
			System.err.println("buffer reader failed toread the file");
			e.printStackTrace();
		 }
	
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		
    String [] Splits = value.toString().split("\\W+");
try{
    for(String word:Splits)
    {
    	if(word.length()>0 && !clientWord.contains(word))
    	{
    		Keyout.set(word);
    		context.write(Keyout,Valueout);
    	}
    }
}catch(IOException e){
	System.err.println("buffer reader failed toread the file");
		e.printStackTrace();
	   }
	}
}
