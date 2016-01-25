package org.apache.hadoop.hive.ql.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

@Description(
		name = "termgenerator",
		value = "_FUNC_(str) - Converts a pattern string to multiple terms",
		extended = "Example:\n" +
		"  > SELECT termgenerator(pattern) as (seq, term) FROM pattern_table a;\n" +
		"  010, 10\n" + 
		"  010, 00\n" +
		"  010, 01\n"
		)
public class TermGeneratorUDTFTest {

	@Test
    public void testUDTFOneSpace() {

	    // set up the models we need
		TermGeneratorUDTF example = new TermGeneratorUDTF();
	    
	    ObjectInspector[] inputOI = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};
	   
	    // create the actual UDF arguments
	    String pattern = "010";

	    // the value exists
	    try{
	    	example.initialize(inputOI);
	    }catch(Exception ex){

	    }
	 
	    ArrayList<Object[]> results = example.processInputRecord(pattern);
	    Assert.assertEquals(3, results.size());
	    Assert.assertEquals("10", results.get(0)[1]);
	    Assert.assertEquals("00", results.get(1)[1]);
	    Assert.assertEquals("01", results.get(2)[1]);
    }
}
