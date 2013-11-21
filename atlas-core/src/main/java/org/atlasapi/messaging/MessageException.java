package org.atlasapi.messaging;

import java.io.IOException;

public class MessageException extends IOException {

    private static final long serialVersionUID = 1L;

    public MessageException() {
        super();
    }

    public MessageException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessageException(String message) {
        super(message);
    }

    public MessageException(Throwable cause) {
        super(cause);
    }

}
