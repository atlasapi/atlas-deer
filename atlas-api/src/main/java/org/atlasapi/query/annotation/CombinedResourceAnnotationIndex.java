package org.atlasapi.query.annotation;

import org.atlasapi.query.common.exceptions.InvalidAnnotationException;

import com.google.common.collect.ImmutableSetMultimap;

final class CombinedResourceAnnotationIndex implements
        ContextualAnnotationIndex {

    private final Index index;

    public CombinedResourceAnnotationIndex(ImmutableSetMultimap<String, PathAnnotation> bindings) {
        this.index = Index.create(bindings);
    }

    @Override
    public ActiveAnnotations resolve(Iterable<String> keys)
            throws InvalidAnnotationException {
        return index.resolve(keys);
    }
}
