package org.atlasapi.query.annotation;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.query.common.exceptions.InvalidAnnotationException;
import org.atlasapi.query.common.exceptions.MissingAnnotationException;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexContextualAnnotationsExtractor implements ContextualAnnotationsExtractor {

    private static final String DEFAULT_PARAMETER_NAME = "annotations";

    private final ContextualAnnotationIndex lookup;
    private final String parameterName;

    private final Splitter csvSplitter = Splitter.on(",").omitEmptyStrings().trimResults();

    private IndexContextualAnnotationsExtractor(
            String parameterName,
            ContextualAnnotationIndex lookup
    ) {
        this.parameterName = checkNotNull(parameterName);
        this.lookup = checkNotNull(lookup);
    }

    public static IndexContextualAnnotationsExtractor create(ContextualAnnotationIndex lookup) {
        return new IndexContextualAnnotationsExtractor(DEFAULT_PARAMETER_NAME, lookup);
    }

    @Override
    public ActiveAnnotations extractFromRequest(HttpServletRequest request)
            throws InvalidAnnotationException, MissingAnnotationException {

        String serialisedAnnotations = request.getParameter(parameterName);

        if (serialisedAnnotations == null) {
            return ActiveAnnotations.standard();
        }

        return lookup.resolve(csvSplitter.split(serialisedAnnotations));
    }

    @Override
    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(parameterName);
    }
}
