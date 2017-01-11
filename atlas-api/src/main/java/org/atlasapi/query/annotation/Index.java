package org.atlasapi.query.annotation;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.exceptions.InvalidAnnotationException;
import org.atlasapi.query.common.validation.ReplacementSuggestion;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

final class Index {

    private final Multimap<String, PathAnnotation> bindings;
    private final ReplacementSuggestion replacementSuggestion;

    private Index(Multimap<String, PathAnnotation> bindings) {
        this.bindings = bindings;
        this.replacementSuggestion = replacements(bindings.keySet());
    }

    public static Index create(Multimap<String, PathAnnotation> bindings) {
        return new Index(bindings);
    }

    private ReplacementSuggestion replacements(Set<String> valid) {
        return ReplacementSuggestion.create(
                valid,
                "Invalid annotations: ",
                " (did you mean %s?)");
    }

    ActiveAnnotations resolve(Iterable<String> keys)
            throws InvalidAnnotationException {
        ImmutableSetMultimap.Builder<List<Resource>, Annotation> annotations =
                ImmutableSetMultimap.builder();

        List<String> invalid = Lists.newArrayList();
        for (String key : keys) {
            Collection<PathAnnotation> paths = getBindings().get(key);
            if (paths == null || paths.isEmpty()) {
                invalid.add(key);
            } else {
                for (PathAnnotation pathAnnotation : paths) {
                    annotations.putAll(pathAnnotation.getPath(), pathAnnotation.getAnnotation());
                }
            }
        }

        if (!invalid.isEmpty()) {
            throw new InvalidAnnotationException(
                    replacementSuggestion.forInvalid(invalid),
                    invalid
            );
        }

        return new ActiveAnnotations(annotations.build());
    }

    Multimap<String, PathAnnotation> filterBindings(String keyPrefix) {
        return Multimaps.filterKeys(
                getBindings(),
                input -> input.startsWith(keyPrefix)
        );
    }

    Multimap<String, PathAnnotation> getBindings() {
        return bindings;
    }
}
