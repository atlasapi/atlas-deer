package org.atlasapi.query.annotation;

import com.google.common.collect.ImmutableSetMultimap;

public final class IndexCombination {

    private static final boolean EXPLICIT = true;
    private static final boolean IMPLICIT = false;

    private static final boolean SINGLE = true;
    private static final boolean LIST = false;

    private final ImmutableSetMultimap.Builder<String, PathAnnotation> bindings
            = ImmutableSetMultimap.builder();

    public IndexCombination() {
    }

    public IndexCombination addExplicitSingleContext(ResourceAnnotationIndex index) {
        add(index, EXPLICIT, SINGLE);
        return this;
    }

    public IndexCombination addImplicitSingleContext(ResourceAnnotationIndex index) {
        add(index, IMPLICIT, SINGLE);
        return this;
    }

    public IndexCombination addExplicitListContext(ResourceAnnotationIndex index) {
        add(index, EXPLICIT, LIST);
        return this;
    }

    public IndexCombination addImplicitListContext(ResourceAnnotationIndex index) {
        add(index, IMPLICIT, LIST);
        return this;
    }

    private void add(ResourceAnnotationIndex index, boolean explicit, boolean single) {
        Index source = single ? index.singleIndex : index.listIndex;
        String prefix = single ? index.resource.getSingular()
                               : index.resource.getPlural();
        this.bindings.putAll(explicit ? source.filterBindings(prefix)
                                      : source.getBindings());
    }

    public ContextualAnnotationIndex combine() {
        return new CombinedResourceAnnotationIndex(bindings.build());
    }
}