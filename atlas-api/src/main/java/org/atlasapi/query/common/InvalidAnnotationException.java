package org.atlasapi.query.common;

import org.atlasapi.content.QueryParseException;

import java.util.List;

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
