package org.apache.hadoop.hive.ql.udtf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(
		name = "termgenerator2",
		value = "_FUNC_(str) - Converts a id string to multiple terms string",
		extended = "Example:\n" +
		"  > SELECT termgenerator(id) as (term) FROM pattern_table a limit 1000;\n" +
		"  0000003003304030001063200033000300047000073200110, 1_0\n" + 
		"  0000003003304030001063200033000300047000073200110, 2_0\n" +
		"  0000003003304030001063200033000300047000073200110, 3_0\n" + 
		"... ..."
		)
public class TermGeneratorUDTF2 extends GenericUDTF {

	private PrimitiveObjectInspector stringOI = null;

	  @Override
	  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

	    if (args.length != 1) {
	      throw new UDFArgumentException("TermGenerator() takes exactly one argument");
	    }

	    if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
	        && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
	      throw new UDFArgumentException("TermGenerator() takes a string as a parameter");
	    }
      
	    // input inspectors
	    stringOI = (PrimitiveObjectInspector) args[0];

	    // output inspectors -- an object with two fields!
	    List<String> fieldNames = new ArrayList<String>(1);
	    List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);
	    
	    fieldNames.add("term");
	    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	  }
	  
	  @Override
	  public void process(Object[] record) throws HiveException {

	    final String pattern = stringOI.getPrimitiveJavaObject(record[0]).toString();

	    ArrayList<Object[]> results = processInputRecord(pattern);

	    Iterator<Object[]> it = results.iterator();
	    
	    while (it.hasNext()){
	    	Object[] r = it.next();
	    	forward(r);
	    }
	  }
	  	  
	  public ArrayList<Object[]> processInputRecord(String pattern){
		    ArrayList<Object[]> result = new ArrayList<Object[]>();
		  
		    // ignoring null or empty input
		    if (pattern == null || pattern.isEmpty()) {
		      return result;
		    }
		    StringBuffer sb = new StringBuffer(pattern);
		    
		    for(int i = 0; i < pattern.length(); i++){
		    	result.add(new Object[] {(i+1) + "S" + sb.charAt(i)});
		    }
		    return result;
	  }

	  @Override
	  public void close() throws HiveException {
	    // do nothing
	  }
}
