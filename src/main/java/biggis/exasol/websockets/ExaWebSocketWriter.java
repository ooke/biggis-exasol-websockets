package biggis.exasol.websockets;

import java.util.List;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;

public class ExaWebSocketWriter extends ExaWebSocketAdapter {
    private String schema = null;
    private String insertStatement = null;
    private Integer statementHandle = null;
    private JsonObject parameterData = null;

    ExaWebSocketWriter(String url, String user, String passwd, String schema, String insertStatement) throws Exception {
	this(url, user, passwd, schema, insertStatement, false);
    }

    ExaWebSocketWriter(String url, String user, String passwd, String schema, String insertStatement, boolean debug) throws Exception {
	super(url, user, passwd, debug);
	this.schema = schema;
	this.insertStatement = insertStatement;

	try {
	    JsonObject loginData = connect();
	    if (debug) System.out.println("Loggedin successfully" + jsonPP(loginData));
	
	    execute("OPEN SCHEMA " + schema);

	    JsonObject rep = communicate(Json.createObjectBuilder()
					 .add("command", "createPreparedStatement")
					 .add("sqlText", insertStatement)
					 .build());
	    statementHandle = rep.getInt("statementHandle");
	    parameterData = rep.getJsonObject("parameterData");
	    if (debug) System.out.println("Statement parameters" + jsonPP(parameterData));
	} catch (Exception err) {
	    try { disconnect(); }
	    catch (Exception se) { }
	    throw err;
	}
    }

    public JsonObject getParameterData() {
	return parameterData;
    }

    @SuppressWarnings("unchecked")
    private <T extends List<?>> T cast(Object obj) {
	return (T) obj;
    }

    public JsonObject write(List<Object> data) throws Exception {
	JsonArrayBuilder datB = Json.createArrayBuilder();
	int numRows = 1;
	boolean single = false, list = false;
	for (Object col: data) {
	    if (col == null) {
		if (list) throw new ExaWebSocketException("All columns neet to be single values or list of values");
		datB.add(Json.createArrayBuilder().addNull().build());
	    } else if (col instanceof Integer) {
		if (list) throw new ExaWebSocketException("All columns neet to be single values or list of values");
		datB.add(Json.createArrayBuilder().add((Integer) col).build());
		single = true;
	    } else if (col instanceof String) {
		if (list) throw new ExaWebSocketException("All columns neet to be single values or list of values");
		datB.add(Json.createArrayBuilder().add((String) col).build());
		single = true;
	    } else if (col instanceof Float) {
		if (list) throw new ExaWebSocketException("All columns neet to be single values or list of values");
		datB.add(Json.createArrayBuilder().add((Float) col).build());
		single = true;
	    } else if (col instanceof List) {
		if (single) throw new ExaWebSocketException("All columns neet to be single values or list of values");
		JsonArrayBuilder rowB = Json.createArrayBuilder();
		List colList = (List) col;
		if (!list) {
		    numRows = colList.size();
		    list = true;
		} else if (numRows != colList.size()) {
		    throw new ExaWebSocketException("All columns need to have same number of elements");
		}
		if (colList.get(0) instanceof Integer) {
		    List<Integer> rowList = cast(col);
		    for (Integer row: rowList) {
			if (row == null) rowB.addNull();
			else rowB.add(row);
		    }
		} else if (colList.get(0) instanceof String) {
		    List<String> rowList = cast(col);
		    for (String row: rowList) {
			if (row == null) rowB.addNull();
			else rowB.add(row);
		    }
		} else if (colList.get(0) instanceof Float) {
		    List<Float> rowList = cast(col);
		    for (Float row: rowList) {
			if (row == null) rowB.addNull();
			else rowB.add(row);
		    }
		}
		datB.add(rowB.build());
	    } else throw new ExaWebSocketException("Unsupported data type");
	}
	JsonObject repB = Json.createObjectBuilder()
	    .add("command", "executePreparedStatement")
	    .add("statementHandle", statementHandle)
	    .add("numColumns", parameterData.getInt("numColumns"))
	    .add("numRows", numRows)
	    .add("columns", parameterData.getJsonArray("columns"))
	    .add("data", datB.build())
	    .build();
	return communicate(repB);
    }

    public void disconnect() throws Exception {
	try {
	    communicate(Json.createObjectBuilder()
			.add("command", "closePreparedStatement")
			.add("statementHandle", statementHandle)
			.build());
	    execute("COMMIT");
	} finally {
	    super.disconnect();
	}
    }
}
