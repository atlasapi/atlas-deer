package org.atlasapi.query.annotation;

import org.atlasapi.query.common.exceptions.InvalidAnnotationException;

public interface AnnotationIndex {

    ActiveAnnotations resolveListContext(Iterable<String> keys)
            throws InvalidAnnotationException;

    ActiveAnnotations resolveSingleContext(Iterable<String> keys)
            throws InvalidAnnotationException;

}
