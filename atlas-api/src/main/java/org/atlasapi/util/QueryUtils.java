package org.atlasapi.util;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.output.OutputContext;

import com.google.common.base.Splitter;

public class QueryUtils {

    public static boolean contextHasAnnotation(OutputContext ctxt, Annotation annotation) {

        return !com.google.common.base.Strings.isNullOrEmpty(ctxt.getRequest().getParameter("annotations"))
                &&
                Splitter.on(',')
                        .splitToList(
                                ctxt.getRequest().getParameter("annotations")
                        )
                        .stream()
                        .anyMatch(input -> input.contains(annotation.toKey()));
    }

}
