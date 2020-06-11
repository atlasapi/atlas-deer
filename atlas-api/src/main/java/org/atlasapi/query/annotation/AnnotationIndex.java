package org.atlasapi.query.annotation;

import org.atlasapi.query.common.exceptions.InvalidAnnotationException;
import org.atlasapi.query.common.exceptions.MissingAnnotationException;

public interface AnnotationIndex {

    ActiveAnnotations resolveListContext(Iterable<String> keys)
            throws InvalidAnnotationException, MissingAnnotationException;

    ActiveAnnotations resolveSingleContext(Iterable<String> keys)
            throws InvalidAnnotationException, MissingAnnotationException;

}
