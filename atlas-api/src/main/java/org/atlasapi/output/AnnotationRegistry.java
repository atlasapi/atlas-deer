package org.atlasapi.output;

import java.util.List;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.output.annotation.OutputAnnotation;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class AnnotationRegistry<T, R> {

    public static final <T> Builder<T> builder() {
        return new Builder<T>();
    }

    public static final class Builder<T> {

        private final BiMap<Annotation, OutputAnnotation<? super T>> annotationMap = HashBiMap.create();
        private final ImmutableSetMultimap.Builder<OutputAnnotation<? super T>, OutputAnnotation<? super T>> implications = ImmutableSetMultimap
                .builder();
        private final ImmutableSetMultimap.Builder<OutputAnnotation<? super T>, OutputAnnotation<? super T>> overrides = ImmutableSetMultimap
                .builder();
        private final ImmutableList.Builder<OutputAnnotation<? super T>> defaults = ImmutableList.builder();

        public Builder<T> register(Annotation annotation, OutputAnnotation<? super T> output) {
            annotationMap.put(annotation, output);
            return this;
        }

        public Builder<T> registerDefault(Annotation annotation,
                OutputAnnotation<? super T, R> output) {
            this.defaults.add(output);
            return register(annotation, output);
        }

        public Builder<T> register(Annotation annotation,
                OutputAnnotation<? super T, R> outputAnnotation, Annotation implied) {
            return register(annotation, outputAnnotation, ImmutableSet.of(implied));
        }

        public Builder<T> register(Annotation annotation, OutputAnnotation<? super T, R> output,
                Iterable<Annotation> implieds) {
            checkRegistered(implieds, "Cannot imply un-registered annotation '%s'");
            register(annotation, output);
            for (Annotation implied : implieds) {
                OutputAnnotation<? super T, R> impliedOut = annotationMap.get(implied);
                implications.put(output, impliedOut);
            }
            return this;
        }

        public Builder<T> register(Annotation annotation, OutputAnnotation<? super T, R> output,
                Iterable<Annotation> implieds, Iterable<Annotation> overridden) {
            checkRegistered(overridden, "Cannot override un-registered annotation '%s'");
            register(annotation, output, implieds);
            overrides.putAll(output, toOutput(overridden));
            return this;
        }

        public AnnotationRegistry<T, R> build() {
            return new AnnotationRegistry<T, R>(
                    annotationMap,
                    implications.build(),
                    overrides.build(),
                    defaults.build()
            );
        }

        private Iterable<OutputAnnotation<? super T, R>> toOutput(Iterable<Annotation> annotations) {
            return Iterables.transform(annotations, Functions.forMap(annotationMap));
        }

        private void checkRegistered(Iterable<Annotation> annotations, String errMsg) {
            for (Annotation annotation : annotations) {
                Preconditions.checkArgument(
                        annotationMap.containsKey(annotation),
                        errMsg,
                        annotation
                );
            }
        }
    }

    private final SetMultimap<OutputAnnotation<? super T>, OutputAnnotation<? super T>> implications;
    private final SetMultimap<OutputAnnotation<? super T>, OutputAnnotation<? super T>> overrides;
    private final BiMap<Annotation, OutputAnnotation<? super T>> annotationMap;
    private final Ordering<OutputAnnotation<? super T>> ordering;
    private final ImmutableList<OutputAnnotation<? super T>> defaults;

    private AnnotationRegistry(BiMap<Annotation, OutputAnnotation<? super T>> annotationMap,
            SetMultimap<OutputAnnotation<? super T>, OutputAnnotation<? super T>> implications,
            SetMultimap<OutputAnnotation<? super T>, OutputAnnotation<? super T>> overrides,
            ImmutableList<OutputAnnotation<? super T>> defaults) {
        this.annotationMap = annotationMap;
        this.implications = implications;
        this.overrides = overrides;
        this.defaults = defaults;
        this.ordering = Ordering.explicit(Annotation.all()
                .asList())
                .onResultOf(Functions.forMap(annotationMap.inverse()));
    }

    public List<OutputAnnotation<? super T>> activeAnnotations(Iterable<Annotation> annotations) {
        ImmutableList.Builder<OutputAnnotation<? super T>> writers = ImmutableList.builder();
        for (Annotation annotation : annotations) {
            OutputAnnotation<? super T> writer = annotationMap.get(annotation);
            if (writer != null) {
                writers.add(writer);
            }
        }
        List<OutputAnnotation<? super T>> flattened = flatten(writers.build());
        return ordering.immutableSortedCopy(flattened);
    }

    private List<OutputAnnotation<? super T>> flatten(
            Iterable<OutputAnnotation<? super T>> annotations) {
        Set<OutputAnnotation<? super T>> activeAnnotations = Sets.newHashSet();
        for (OutputAnnotation<? super T> annotation : annotations) {
            addWithImplied(activeAnnotations, annotation);
        }
        for (OutputAnnotation<? super T> annotation : annotations) {
            for (Object overridden : overrides.get(annotation)) {
                activeAnnotations.remove(overridden);
            }
        }
        return ImmutableList.copyOf(activeAnnotations);
    }

    @SuppressWarnings("unchecked")
    private void addWithImplied(Set<OutputAnnotation<? super T>> builder,
            OutputAnnotation<? super T> annotation) {
        for (OutputAnnotation<?> implied : implications.get(annotation)) {
            addWithImplied(builder, (OutputAnnotation<? super T>) implied);
        }
        builder.add(annotation);
    }

    public List<OutputAnnotation<? super T>> defaultAnnotations() {
        return defaults;
    }

}
