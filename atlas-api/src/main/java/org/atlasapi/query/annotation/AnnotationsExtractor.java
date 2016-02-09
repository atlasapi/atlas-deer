package org.atlasapi.query.annotation;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.query.common.InvalidAnnotationException;

import com.google.common.collect.ImmutableSet;

public interface AnnotationsExtractor {

    ActiveAnnotations extractFromSingleRequest(HttpServletRequest request)
            throws InvalidAnnotationException;

    ActiveAnnotations extractFromListRequest(HttpServletRequest request)
            throws InvalidAnnotationException;

    ImmutableSet<String> getParameterNames();

}