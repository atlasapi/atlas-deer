package org.atlasapi.query.annotation;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.query.common.InvalidAnnotationException;
import org.atlasapi.query.common.ReplacementSuggestion;
import org.atlasapi.query.common.Resource;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

final class Index {

    private final Multimap<String, PathAnnotation> bindings;
    private final ReplacementSuggestion replacementSuggestion;

    public Index(Multimap<String, PathAnnotation> bindings) {
        this.bindings = bindings;
        this.replacementSuggestion = replacements(bindings.keySet());
    }

    private ReplacementSuggestion replacements(Set<String> valid) {
        return new ReplacementSuggestion(
                valid, "Invalid annotations: ", " (did you mean %s?)");
    }

    ActiveAnnotations resolve(Iterable<String> keys)
            throws InvalidAnnotationException {
        ImmutableSetMultimap.Builder<List<Resource>, Annotation> annotations = ImmutableSetMultimap.builder();
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
        return Multimaps.filterKeys(getBindings(), startsWith(keyPrefix));
    }

    private Predicate<? super String> startsWith(final String prefix) {
        return new Predicate<String>() {

            @Override
            public boolean apply(String input) {
                return input.startsWith(prefix);
            }
        };
    }

    Multimap<String, PathAnnotation> getBindings() {
        return bindings;
    }
}