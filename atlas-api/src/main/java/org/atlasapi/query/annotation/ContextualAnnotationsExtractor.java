package org.atlasapi.query.annotation;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.query.common.InvalidAnnotationException;

import com.google.common.collect.ImmutableSet;

public interface ContextualAnnotationsExtractor {

    ActiveAnnotations extractFromRequest(HttpServletRequest request)
            throws InvalidAnnotationException;

    ImmutableSet<String> getParameterNames();

}