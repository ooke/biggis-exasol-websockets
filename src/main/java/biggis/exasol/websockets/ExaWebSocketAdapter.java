package biggis.exasol.websockets;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Base64;
import java.io.StringWriter;
import java.io.StringReader;
import java.math.BigInteger;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
import javax.crypto.Cipher;
import java.security.KeyFactory;
import java.security.spec.RSAPublicKeySpec;
import java.security.interfaces.RSAPublicKey;

import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketState;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFrame;
import com.neovisionaries.ws.client.WebSocketFactory;
import com.neovisionaries.ws.client.ThreadType;

public class ExaWebSocketAdapter extends WebSocketAdapter {
    private JsonWriterFactory jsonWriterFactoryPP;
    private JsonObject message = null;
    private Throwable error = null;

    public JsonObject attributes = null;
    public JsonObject loginData = null;

    private enum State { INITIALIZING, CONNECTING, LOGIN, ACTIVE };
    private State state;
    private String url, user, passwd;
    private boolean debug = false;
    private WebSocket websocket = null;

    ExaWebSocketAdapter(String url, String user, String passwd) {
	super();
	this.initialize(url, user, passwd, false);
    }

    ExaWebSocketAdapter(String url, String user, String passwd, boolean debug) {
	super();
	initialize(url, user, passwd, debug);
    }

    private void initialize(String url, String user, String passwd, boolean debug) {
	this.url = url;
	this.user = user;
	this.passwd = passwd;
	this.debug = debug;

	Map<String, Object> jsonpp = new HashMap<String, Object>(1);
	jsonpp.put(JsonGenerator.PRETTY_PRINTING, true);
	this.jsonWriterFactoryPP = Json.createWriterFactory(jsonpp);
	this.state = State.INITIALIZING;

    }

    public JsonObject connect() throws Exception {
	this.websocket = new WebSocketFactory()
	    .createSocket(url)
	    .addListener(this)
	    .setPingInterval(20 * 1000)
	    .connect();
	return this.receive();
    }

    public void disconnect() throws Exception {
	this.websocket.disconnect();
    }

    public String jsonPP(JsonObject json) {
	StringWriter sw = new StringWriter();
	JsonWriter jw = this.jsonWriterFactoryPP.createWriter(sw);
	jw.writeObject(json);
	jw.close();
	return sw.toString();
    }

    public String jsonPP(JsonArray json) {
	StringWriter sw = new StringWriter();
	JsonWriter jw = this.jsonWriterFactoryPP.createWriter(sw);
	jw.writeArray(json);
	jw.close();
	return sw.toString();
    }

    private synchronized void receivedMessage(JsonObject message) {
	while (this.message != null && this.error != null) {
	    try { wait(); }
	    catch (InterruptedException e) { }
	}
	this.message = message;
	notify();
    }

    private synchronized void receivedError(Throwable error) {
	while (this.message != null && this.error != null) {
	    try { wait(); }
	    catch (InterruptedException e) { }
	}
	this.error = error;
	notify();
    }

    public synchronized JsonObject receive() throws Exception {
	while (this.message == null && this.error == null) {
	    try { wait(); }
	    catch (InterruptedException e) { continue; }
	}
	if (this.error != null) {
	    Throwable error = this.error;
	    this.error = null;
	    notify();
	    throw new ExaWebSocketException(error);
	}
	JsonObject message = this.message;
	this.message = null;
	notify();
	return message;
    }

    public void send(JsonObject message) throws Exception {
	if (debug) System.out.println("Send message" + jsonPP(message));
	this.websocket.sendText(message.toString());
    }

    public JsonObject communicate(JsonObject message) throws Exception {
	send(message);
	return this.receive();
    }

    public JsonObject execute(String sqlText) throws Exception {
	send(Json.createObjectBuilder()
	     .add("command", "execute")
	     .add("sqlText", sqlText)
	     .build());
	return this.receive();
    }

    public void handleCallbackError(WebSocket websocket, Throwable cause) throws Exception {
	if (debug) {
	    System.err.println("Caught exception: " + cause);
	    if (this.message != null)
		System.err.println("Message still here");
	}
	this.receivedError(cause);
    }

    private String encryptPassword(BigInteger pkMod, BigInteger pkExp) throws Exception {
	RSAPublicKeySpec spec = new RSAPublicKeySpec(pkMod, pkExp);
	RSAPublicKey key = (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(spec);
	Cipher cipher = Cipher.getInstance("RSA");
	cipher.init(Cipher.ENCRYPT_MODE, key);
	byte[] data = cipher.doFinal(this.passwd.getBytes("UTF-8"));
	return new String(Base64.getEncoder().encode(data));
    }

    private void handleLogin(WebSocket websocket, String publicKeyMod, String publicKeyExp) throws Exception {
	if (debug) System.out.println("handleLogin caled");
	BigInteger pkMod = new BigInteger(publicKeyMod, 16);
	BigInteger pkExp = new BigInteger(publicKeyExp, 16);
	JsonObject req = Json.createObjectBuilder()
	    .add("username", this.user)
	    .add("password", encryptPassword(pkMod, pkExp))
	    .add("useCompression", false)
	    .add("clientName", "EXAJWS")
	    .add("driverName", "WS")
	    .add("clientOs", System.getProperty("os.name"))
	    .add("clientOsUsername", System.getProperty("user.name"))
	    .add("clientVersion", "1.0")
	    .add("clientRuntime", "Java " + System.getProperty("java.version"))
	    .build();
	this.passwd = null;
	if (debug) System.out.println(this.jsonPP(req));
	websocket.sendText(req.toString());
    }

    public void onStateChanged(WebSocket websocket, WebSocketState newState) throws Exception {
	if (debug) {
	    System.out.print("onStateChanged caled, newState: ");
	    System.out.println(newState);
	}
    }

    public void onConnected(WebSocket websocket, Map<String, List<String>> headers) throws Exception {
	if (debug) {
	    System.out.println("onConnected caled, headers: ");
	    for (Map.Entry<String, List<String>> entry: headers.entrySet()) {
		System.out.print("    ");
		System.out.println(entry.getKey());
		for (String value: entry.getValue()) {
		    System.out.print("        ");
		    System.out.println(value);
		}
	    }
	}
	JsonObject req = Json.createObjectBuilder()
	    .add("command", "login")
	    .add("protocolVersion", 1)
	    .build();
	if (debug) {
	    System.out.print("Send request:");
	    System.out.println(this.jsonPP(req));
	}
	websocket.sendText(req.toString());
	this.state = State.CONNECTING;
    }

    public void onConnectError(WebSocket websocket, WebSocketException cause) throws Exception {
	if (debug) {
	    System.out.print("onConnectError caled, cause: ");
	    System.out.println(cause);
	}
    }

    public void onDisconnected(WebSocket websocket, WebSocketFrame serverCloseFrame, WebSocketFrame clientCloseFrame, boolean closedByServer) throws Exception {
	if (debug) System.out.println("onDisconnected caled");
    }

    public void onFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onFrame caled");
    }

    public void onContinuationFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onContinuationFrame caled");
    }

    public void onTextFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onTextFrame caled");
    }

    public void onBinaryFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onBinaryFrame caled");
    }

    public void onCloseFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onCloseFrame caled");
    }

    public void onPingFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onPingFrame caled");
    }

    public void onPongFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onPongFrame caled");
    }

    public void onTextMessage(WebSocket websocket, String text) throws Exception {
	if (debug) System.out.print("onTextMessage caled, text:");
	JsonObject rep = Json.createReader(new StringReader(text)).readObject();
	if (debug) System.out.println(this.jsonPP(rep));
	if (rep.getJsonString("status") == null) {
	    this.receivedError(new ExaWebSocketException("No valid status in response"));
	    return;
	}
	String status = rep.getString("status");
	JsonObject repErr = rep.getJsonObject("exception");
	if (status.equals("error")) {
	    if (repErr == null)
		this.receivedError(new ExaWebSocketException("Uknown internal operational error"));
	    else this.receivedError(new ExaWebSocketException(String.format("Operational error [%s] %s",
									    repErr.getString("sqlCode"),
									    repErr.getString("text"))));
	    return;
	} else if (!status.equals("ok")) {
	    this.receivedError(new ExaWebSocketException("Got unknown status: " + status));
	    return;
	}
	if (repErr != null) {
	    this.receivedError(new ExaWebSocketException(String.format("Database error [%s] %s",
								       repErr.getString("sqlCode"),
								       repErr.getString("text"))));
	    return;
	}
	JsonObject data = rep.getJsonObject("responseData");
	JsonObject attrs = rep.getJsonObject("attributes");
	if (data == null)
	    data = Json.createObjectBuilder().build();
	switch (this.state) {
	case INITIALIZING:
	    this.receivedError(new ExaWebSocketException("Internal error"));
	    break;
	case CONNECTING:
	    this.handleLogin(websocket, data.getString("publicKeyModulus"), data.getString("publicKeyExponent"));
	    this.state = State.LOGIN;
	    break;
	case LOGIN:
	    this.loginData = data;
	    this.receivedMessage(data);
	    this.state = State.ACTIVE;
	    break;
	case ACTIVE:
	    if (attrs != null)
		this.attributes = attrs;
	    this.receivedMessage(data);
	    break;
	}
    }

    public void onBinaryMessage(WebSocket websocket, byte[] binary) throws Exception {
	if (debug) System.out.println("onBinaryMessage caled");
    }

    public void onSendingFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onSendingFrame caled");
    }

    public void onFrameSent(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onFrameSent caled");
    }

    public void onFrameUnsent(WebSocket websocket, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onFrameUnsent caled");
    }

    public void onThreadCreated(WebSocket websocket, ThreadType threadType, Thread thread) throws Exception {
	if (debug) {
	    System.out.print("onThreadCreated caled, thread: ");
	    System.out.println(thread.getName());
	}
    }

    public void onThreadStarted(WebSocket websocket, ThreadType threadType, Thread thread) throws Exception {
	if (debug) {
	    System.out.print("onThreadStarted caled, thread: ");
	    System.out.println(thread.getName());
	}
    }

    public void onThreadStopping(WebSocket websocket, ThreadType threadType, Thread thread) throws Exception {
	if (debug) {
	    System.out.print("onThreadStopping caled, thread: ");
	    System.out.println(thread.getName());
	}
    }

    public void onError(WebSocket websocket, WebSocketException cause) throws Exception {
	if (debug) {
	    System.out.print("onError caled, cause: ");
	    System.out.println(cause);
	}
    }

    public void onFrameError(WebSocket websocket, WebSocketException cause, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onFrameError caled");
    }

    public void onMessageError(WebSocket websocket, WebSocketException cause, List<WebSocketFrame> frames) throws Exception {
	if (debug) System.out.println("onMessageError caled");
    }

    public void onMessageDecompressionError(WebSocket websocket, WebSocketException cause, byte[] compressed) throws Exception {
	if (debug) System.out.println("onMessageDecompressionError caled");
    }

    public void onTextMessageError(WebSocket websocket, WebSocketException cause, byte[] data) throws Exception {
	if (debug) System.out.println("onTextMessageError caled");
    }

    public void onSendError(WebSocket websocket, WebSocketException cause, WebSocketFrame frame) throws Exception {
	if (debug) System.out.println("onSendError caled");
    }

    public void onUnexpectedError(WebSocket websocket, WebSocketException cause) throws Exception {
	if (debug) System.out.println("onUnexpectedError caled");
    }

    public void onSendingHandshake(WebSocket websocket, String requestLine, List<String[]> headers) throws Exception {
	if (debug) System.out.println("onSendingHandshake caled");
    }
}
