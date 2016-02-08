package org.atlasapi.query.annotation;

import org.atlasapi.query.common.InvalidAnnotationException;

public interface ContextualAnnotationIndex {

    ActiveAnnotations resolve(Iterable<String> keys) throws InvalidAnnotationException;

}
