package Booksxml;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;

public class BookinputMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	private String author= null;
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		InputSource source = new InputSource(new StringReader(value.toString().replaceAll("<catalog>", "")+"</book>"));
		XPath xpath = XPathFactory.newInstance().newXPath();
		Object book;
		try {
			book = xpath.evaluate("/book", source, XPathConstants.NODE);
			author = xpath.evaluate("author", book);
		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
context.write(key, new Text(author));
		
	}
	
	

}
