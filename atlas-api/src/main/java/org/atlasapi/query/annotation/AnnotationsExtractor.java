package org.atlasapi.query.annotation;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.query.common.exceptions.InvalidAnnotationException;
import org.atlasapi.query.common.exceptions.MissingAnnotationException;

import com.google.common.collect.ImmutableSet;

public interface AnnotationsExtractor {

    ActiveAnnotations extractFromSingleRequest(HttpServletRequest request)
            throws InvalidAnnotationException, MissingAnnotationException;

    ActiveAnnotations extractFromListRequest(HttpServletRequest request)
            throws InvalidAnnotationException, MissingAnnotationException;

    ImmutableSet<String> getParameterNames();

}
