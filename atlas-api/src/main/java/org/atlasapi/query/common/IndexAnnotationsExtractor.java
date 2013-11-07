package org.atlasapi.query.common;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.annotation.AnnotationIndex;
import org.atlasapi.query.annotation.AnnotationsExtractor;


import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

public class IndexAnnotationsExtractor implements AnnotationsExtractor {

    private static final String DEFAULT_PARAMETER_NAME = "annotations";

    private final AnnotationIndex lookup;
    private final String parameterName;

    private final Splitter csvSplitter = Splitter.on(",").omitEmptyStrings().trimResults();

    public IndexAnnotationsExtractor(AnnotationIndex lookup) {
        this(DEFAULT_PARAMETER_NAME, lookup);
    }

    public IndexAnnotationsExtractor(String parameterName, AnnotationIndex lookup) {
        this.parameterName = checkNotNull(parameterName);
        this.lookup = checkNotNull(lookup);
    }

    @Override
    public ActiveAnnotations extractFromSingleRequest(HttpServletRequest request)
            throws InvalidAnnotationException {

        String serialisedAnnotations = request.getParameter(parameterName);

        if (serialisedAnnotations == null) {
            return ActiveAnnotations.standard();
        }

        return lookup.resolveSingleContext(csvSplitter.split(serialisedAnnotations));
    }

    @Override
    public ActiveAnnotations extractFromListRequest(HttpServletRequest request)
            throws InvalidAnnotationException {
        
        String serialisedAnnotations = request.getParameter(parameterName);
        
        if (serialisedAnnotations == null) {
            return ActiveAnnotations.standard();
        }
        
        return lookup.resolveListContext(csvSplitter.split(serialisedAnnotations));
    }

    @Override
    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(parameterName);
    }
}
