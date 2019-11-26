package org.atlasapi.query.common.exceptions;

import java.util.List;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.QueryParseException;

public class MissingAnnotationException extends QueryParseException {

    private final List<Annotation> missingAnnotations;

    public MissingAnnotationException(String message, List<Annotation> missingAnnotations) {
        super(message);
        this.missingAnnotations = missingAnnotations;
    }

    public List<Annotation> getMissingAnnotations() {
        return this.missingAnnotations;
    }

}
