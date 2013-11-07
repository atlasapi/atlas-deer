package org.atlasapi.query.common;


public class InvalidAttributeValueException extends QueryParseException {

    public InvalidAttributeValueException() {
        super();
    }

    public InvalidAttributeValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidAttributeValueException(String message) {
        super(message);
    }

    public InvalidAttributeValueException(Throwable cause) {
        super(cause);
    }

}
