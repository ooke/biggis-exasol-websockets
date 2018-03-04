package biggis.exasol.websockets;

public class ExaWebSocketException extends Exception {
    ExaWebSocketException(Throwable e) {
	super(e);
    }
    ExaWebSocketException(Exception e) {
	super(e);
    }
    ExaWebSocketException(String message) {
	super(message);
    }
}
