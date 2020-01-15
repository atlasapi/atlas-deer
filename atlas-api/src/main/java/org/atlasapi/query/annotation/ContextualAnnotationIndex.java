package org.atlasapi.query.annotation;

import org.atlasapi.query.common.exceptions.InvalidAnnotationException;
import org.atlasapi.query.common.exceptions.MissingAnnotationException;

public interface ContextualAnnotationIndex {

    ActiveAnnotations resolve(Iterable<String> keys)
            throws InvalidAnnotationException, MissingAnnotationException;

}
