package org.atlasapi.query.annotation;

import org.atlasapi.query.common.InvalidAnnotationException;

public interface AnnotationIndex {

    ActiveAnnotations resolveListContext(Iterable<String> keys)
            throws InvalidAnnotationException;

    ActiveAnnotations resolveSingleContext(Iterable<String> keys)
            throws InvalidAnnotationException;

}