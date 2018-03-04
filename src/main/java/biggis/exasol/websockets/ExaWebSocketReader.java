package biggis.exasol.websockets;

import java.util.List;
import java.util.ArrayList;
import javax.json.Json;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;

public class ExaWebSocketReader extends ExaWebSocketAdapter {
    private String schema = null;
    private String selectStatement = null;
    private JsonObject result = null;
    private int numColumns = 0, rowsReturned = 0, numRows = 0, rowsCurrent = 0, rowsCurrentPos = 0;
    private int resultSetHandle = 0;

    ExaWebSocketReader(String url, String user, String passwd, String schema, String selectStatement) throws Exception {
	this(url, user, passwd, schema, selectStatement, false);
    }

    ExaWebSocketReader(String url, String user, String passwd, String schema, String selectStatement, boolean debug) throws Exception {
	super(url, user, passwd, debug);
	this.schema = schema;
	this.selectStatement = selectStatement;

	try {
	    JsonObject loginData = connect();
	    if (debug) System.out.println("Loggedin successfully" + jsonPP(loginData));
	
	    execute("OPEN SCHEMA " + schema);

	    JsonObject rep = execute(selectStatement);
	    if (rep.getInt("numResults") != 1)
		throw new ExaWebSocketException("Only single result statements are supported");
	    result = rep.getJsonArray("results").getJsonObject(0);
	    if (!result.getString("resultType").equals("resultSet"))
		throw new ExaWebSocketException("Got invalid result");
	    result = result.getJsonObject("resultSet");
	    numColumns = result.getInt("numColumns");
	    numRows = result.getInt("numRows");
	    rowsCurrent = result.getInt("numRowsInMessage");
	    if (result.getJsonNumber("resultSetHandle") != null)
		resultSetHandle = result.getInt("resultSetHandle");
	    if (debug) System.out.println("Statement result" + jsonPP(result));
	} catch (Exception err) {
	    try { disconnect(); }
	    catch (Exception se) { }
	    throw err;
	}
    }

    public int getNumRows() {
	return numRows;
    }

    public JsonArray getColumns() {
	return result.getJsonArray("columns");
    }

    public boolean end() {
	return rowsReturned == numRows;
    }

    public List<JsonValue> nextRow() throws Exception {
	if (end()) throw new ExaWebSocketException("End of fetchable data");
	if (rowsCurrentPos == rowsCurrent) {
	    result = communicate(Json.createObjectBuilder()
				 .add("command", "fetch")
				 .add("resultSetHandle", resultSetHandle)
				 .add("startPosition", rowsReturned)
				 .add("numBytes", 128*1024)
				 .build());
	    rowsCurrent = result.getInt("numRows");
	    rowsCurrentPos = 0;
	}
	List<JsonValue> ret = new ArrayList<JsonValue>(numColumns);
	for (int i = 0; i < numColumns; ++i)
	    ret.add(result.getJsonArray("data").getJsonArray(i).get(rowsCurrentPos));
	++rowsReturned;
	++rowsCurrentPos;
	return ret;
    }
}
