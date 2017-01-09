package org.atlasapi.query.annotation;

import org.atlasapi.query.common.exceptions.InvalidAnnotationException;

public interface ContextualAnnotationIndex {

    ActiveAnnotations resolve(Iterable<String> keys) throws InvalidAnnotationException;

}
