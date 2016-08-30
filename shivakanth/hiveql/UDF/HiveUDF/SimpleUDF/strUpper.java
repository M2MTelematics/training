package HiveUDF.SimpleUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class strUpper extends UDF{


	public String evaluate(String row){
		return row.toUpperCase();
	}
	
	
}
