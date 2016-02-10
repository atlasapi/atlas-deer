package org.atlasapi.input;

public class ReadException extends Exception {

    public ReadException() {
        super();
    }

    public ReadException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReadException(String message) {
        super(message);
    }

    public ReadException(Throwable cause) {
        super(cause);
    }

}
