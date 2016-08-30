package HiveUDF.GenericUDF;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class VowelSearch extends GenericUDF{

	private ListObjectInspector inputLOI; 
	private List<Text> returnList = new ArrayList<Text>();
	
	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		// TODO Auto-generated method stub
		returnList.clear();
		List<Text> inputList = (List<Text>) inputLOI.getList(args[0].get());
		for(Text word: inputList){ String w = word.toString(); 
		if(!w.isEmpty()){ 
			String firstChar = word.toString().substring(0,1).toLowerCase(); 
		switch(firstChar){ 
		case "a": case "e": case "i": case "o": case "u": returnList.add(word); break; } } } return returnList;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return "not set";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		// TODO Auto-generated method stub
		if(args.length != 1) 
			throw new UDFArgumentLengthException("Expecting only one argument but got:"+args.length+" number of arguments"); 
		if(args[0].getCategory() != ObjectInspector.Category.LIST) 
			throw new UDFArgumentTypeException(0, "Expecting list as an arguement but found:"+args[0].getCategory()); 
		inputLOI = (ListObjectInspector) args[0]; 
		if(inputLOI.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) 
			throw new UDFArgumentException("Expecting primitive type as a list elements but found:"+inputLOI.getListElementObjectInspector().getCategory()); 
		PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inputLOI.getListElementObjectInspector(); 
		if(poi.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) 
			throw new UDFArgumentException("Expecting String as list elements but found:"+poi.getTypeName());
		
		return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

}
