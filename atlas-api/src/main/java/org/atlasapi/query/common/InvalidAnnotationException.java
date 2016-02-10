package org.atlasapi.query.common;

import java.util.List;

import org.atlasapi.content.QueryParseException;

public class InvalidAnnotationException extends QueryParseException {

    private final List<String> invalidAnnotations;

    public InvalidAnnotationException(String message, List<String> invalidAnnotations) {
        super(message);
        this.invalidAnnotations = invalidAnnotations;
    }

    public List<String> getInvalidAnnotations() {
        return this.invalidAnnotations;
    }

}
