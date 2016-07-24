/**
 * 
 */
package com.xg.hadoop.training.distributed.cache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author som.shekhar
 *
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text,Text,Text> {
	private final static String DELIMITER = ",";
	
	private Text opKey = new Text();
	private Text opValue = new Text();
	private HashMap<String, String> locationMap;
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] splits = value.toString().split(DELIMITER);
		if(splits.length> 6){
			String locID = splits[6];
			String locationName = "Unknown";
			if(locationMap.containsKey(locID)){
				locationName = locationMap.get(locID);
			}
			opKey.set(splits[0]);
			opValue.set(locationName);
			context.write(opKey,opValue);
		}
		
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		locationMap = new HashMap<String,String>();
		URI[] files = context.getCacheFiles();
		Path locationFilePath = new Path(files[0]);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream iStream = fs.open(locationFilePath);
		BufferedReader br = new BufferedReader(new InputStreamReader(iStream));
		String line=null;
		while((line = br.readLine())!= null){
			String[] splits = line.split(DELIMITER);
			if(splits.length ==2){
				String locID = splits[0];
				String locationName = splits[1];
				locationMap.put(locID, locationName);
			}
		}
	}
}
