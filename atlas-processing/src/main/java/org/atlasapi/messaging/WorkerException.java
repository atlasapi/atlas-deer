package org.atlasapi.messaging;

public class WorkerException extends RuntimeException {

    private WorkerException(String message) {
        super(message);
    }

    private WorkerException(String message, Throwable cause) {
        super(message, cause);
    }

    public static WorkerException create(String message) {
        return new WorkerException(message);
    }

    public static WorkerException create(String message, Throwable cause) {
        return new WorkerException(message, cause);
    }
}
