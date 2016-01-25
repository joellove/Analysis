package org.apache.hadoop.hive.ql.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

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
public class TermGeneratorUDTF2Test {

	@Test
    public void testUDTFOneSpace() {

	    // set up the models we need
		TermGeneratorUDTF2 example = new TermGeneratorUDTF2();
	    
	    ObjectInspector[] inputOI = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};
	   
	    // create the actual UDF arguments
	    String pattern = "0000003003304030001063200033000300047000073200110";

	    // the value exists
	    try{
	    	example.initialize(inputOI);
	    }catch(Exception ex){

	    }
	 
	    ArrayList<Object[]> results = example.processInputRecord(pattern);
	    Assert.assertEquals(49, results.size());
	    Assert.assertEquals("1S0", results.get(0)[0]);
	    Assert.assertEquals("2S0", results.get(1)[0]);
	    Assert.assertEquals("3S0", results.get(2)[0]);
    }
}
