package biggis.exasol.websockets;

import java.util.List;
import java.util.ArrayList;
import javax.json.JsonValue;
import javax.json.JsonObject;

public class ExaWebSocketTest {
    public static void main(String[] args) throws Exception {
	String url = "ws://localhost:8899";
	String user = "sys";
	String passwd = "exasol";

	System.out.println("=== INITIALIZE TEST ===");
	ExaWebSocketAdapter ews = null;
	try {
	    ews = new ExaWebSocketAdapter(url, user, passwd);

	    // try to connect to the database
	    JsonObject loginData = ews.connect();
	    System.out.println("Loggedin successfully" + ews.jsonPP(loginData));
	    
	    // try to open schema, if it can not be open, try to create it
	    try {
		ews.execute("OPEN SCHEMA TEST");
		System.out.println("Schema opened successfully");
	    } catch (ExaWebSocketException err) {
		if (err.getMessage().contains("schema TEST not found")) {
		    ews.execute("CREATE SCHEMA TEST");
		    ews.execute("COMMIT");
		    System.out.println("Schema created successfully");
		} else throw err;
	    }

	    // create tast table and commit changes
	    ews.execute("CREATE OR REPLACE TABLE testTable (f1 INT, f2 VARCHAR(2000000))");
	    ews.execute("COMMIT");
	    System.out.println("testTable created successfully");

	} finally {
	    try { ews.disconnect(); }
	    catch (Exception err) { }
	    ews = null;
	    System.out.println("=== INITIALIZATION FINISHED ===");
	}

	System.out.println("=== WRITER TEST START ===");
	ExaWebSocketWriter ewswr = null;
	try {
	    // create writer object to write to given SQL expression
	    ewswr = new ExaWebSocketWriter(url, user, passwd, "TEST", "INSERT INTO testTable VALUES (?, ?)");
	    System.out.println("Loggedin successfully" + ewswr.jsonPP(ewswr.loginData));

	    // show parameters for given insert statement after parsing by the database
	    JsonObject parameters = ewswr.getParameterData(), rep = null;
	    System.out.println("Statement prepared with following parameters" + ewswr.jsonPP(parameters));

	    // try to insert one single row of data
	    List<Object> data1 = new ArrayList<Object>(parameters.getInt("numColumns"));
	    data1.add(new Integer(1));
	    data1.add(new String("test1"));
	    rep = ewswr.write(data1);
	    System.out.println("Single value data inserted" + ewswr.jsonPP(rep));

	    // try to insert a block of data (each culumn is an array)
	    List<Integer> col1 = new ArrayList<Integer>(5);
	    col1.add(new Integer(2));
	    col1.add(new Integer(3));
	    col1.add(new Integer(4));
	    col1.add(null);
	    col1.add(new Integer(6));
	    List<String> col2 = new ArrayList<String>(5);
	    col2.add(new String("test2"));
	    col2.add(new String("test3"));
	    col2.add(new String("test4"));
	    col2.add(new String("test5"));
	    col2.add(null);
	    List<Object> data2 = new ArrayList<Object>(parameters.getInt("numColumns"));
	    data2.add(col1);
	    data2.add(col2);
	    rep = ewswr.write(data2);
	    System.out.println("Multi value data inserted" + ewswr.jsonPP(rep));

	    // try to insert one row with nulls
	    List<Object> data3 = new ArrayList<Object>(parameters.getInt("numColumns"));
	    data3.add(null);
	    data3.add(null);
	    rep = ewswr.write(data3);
	    System.out.println("Single value null data inserted" + ewswr.jsonPP(rep));

	    // try to insert one row with a string instead of number (for representig big numbers)
	    List<Object> data4 = new ArrayList<Object>(parameters.getInt("numColumns"));
	    data4.add(new String("7"));
	    data4.add(new String("test7"));
	    rep = ewswr.write(data4);
	    System.out.println("Single value data as strings inserted" + ewswr.jsonPP(rep));
	} finally {
	    try { ewswr.disconnect(); }
	    catch (Exception err) { }
	    ewswr = null;
	    System.out.println("=== WRITER TEST FINISHTED ===");
	}

	System.out.println("=== READER TEST START ===");
	ExaWebSocketReader ewsrd = null;
	try {
	    // create reader to read data from given statement
	    ewsrd = new ExaWebSocketReader(url, user, passwd, "TEST", "SELECT * FROM testTable");
	    System.out.println("Loggedin successfully" + ewsrd.jsonPP(ewsrd.loginData));
	    System.out.println(String.format("Got %d rows with following columns%s", ewsrd.getNumRows(), ewsrd.jsonPP(ewsrd.getColumns())));

	    // read rows one by one
	    while (!ewsrd.end()) {
		List<JsonValue> row = ewsrd.nextRow();
		System.out.println("ROW: " + row);
	    }
	} finally {
	    try { ewsrd.disconnect(); }
	    catch (Exception err) { }
	    ewsrd = null;
	    System.out.println("=== READER TEST FINISHTED ===");
	}
    }
}
