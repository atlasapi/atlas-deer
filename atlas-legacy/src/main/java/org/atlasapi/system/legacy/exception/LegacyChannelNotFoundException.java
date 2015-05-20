package org.atlasapi.system.legacy.exception;

public class LegacyChannelNotFoundException extends RuntimeException {
    public LegacyChannelNotFoundException() {
    }

    public LegacyChannelNotFoundException(String message) {
        super(message);
    }

    public LegacyChannelNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public LegacyChannelNotFoundException(Throwable cause) {
        super(cause);
    }

    public LegacyChannelNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
