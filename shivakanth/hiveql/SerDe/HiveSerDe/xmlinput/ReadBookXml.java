package HiveSerDe.xmlinput;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.xml.sax.InputSource;

public class ReadBookXml extends AbstractSerDe{

	private List<String> colNames;
	private StructTypeInfo rowTypeInfo;
	private ObjectInspector rowOI;
    private List<Object> row = new ArrayList<Object>();

	

	@Override
	public void initialize(Configuration conf, Properties tableProperties) throws SerDeException {
		// TODO Auto-generated method stub
		String colNamesStr = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
		colNames = Arrays.asList(colNamesStr.split(","));
		String colTypesStr = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
		rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
		rowOI =TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
	}

	@Override
	public Object deserialize(Writable record) throws SerDeException {
		// TODO Auto-generated method stub
		
		Text rowText = (Text) record;
		Object value = null;
		
		//InputSource source = new InputSource(new StringReader(rowText.toString().replaceAll("<catalog>", "")+"</book>"));
		InputSource source = new InputSource(rowText.toString());
		XPath xpath = XPathFactory.newInstance().newXPath();
		Object book;
		
		try {
			book = xpath.evaluate("/book", source, XPathConstants.NODE);
			
			for(String fieldName: rowTypeInfo.getAllStructFieldNames())
			{
			//TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
			value = xpath.evaluate(fieldName, book);
			row.add(value);
			}

		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return row;
				
	}


	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		// TODO Auto-generated method stub
		return rowOI;
	}


	@Override
	public SerDeStats getSerDeStats() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Class<? extends Writable> getSerializedClass() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
		// TODO Auto-generated method stub
		return null;
	}


	
}