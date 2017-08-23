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

public class AnnotationRegistry<T, W> {

    public static final <T, W> Builder<T, W> builder() {
        return new Builder<T, W>();
    }

    public static final class Builder<T, W> {

        private final BiMap<Annotation, OutputAnnotation<? super T, W>> annotationMap = HashBiMap.create();
        private final ImmutableSetMultimap.Builder<OutputAnnotation<? super T, W>, OutputAnnotation<? super T, W>> implications = ImmutableSetMultimap
                .builder();
        private final ImmutableSetMultimap.Builder<OutputAnnotation<? super T, W>, OutputAnnotation<? super T, W>> overrides = ImmutableSetMultimap
                .builder();
        private final ImmutableList.Builder<OutputAnnotation<? super T, W>> defaults = ImmutableList.builder();

        public Builder<T, W> register(Annotation annotation, OutputAnnotation<? super T, W> output) {
            annotationMap.put(annotation, output);
            return this;
        }

        public Builder<T, W> registerDefault(Annotation annotation,
                OutputAnnotation<? super T, W> output) {
            this.defaults.add(output);
            return register(annotation, output);
        }

        public Builder<T, W> register(Annotation annotation,
                OutputAnnotation<? super T, W> outputAnnotation, Annotation implied) {
            return register(annotation, outputAnnotation, ImmutableSet.of(implied));
        }

        public Builder<T, W> register(Annotation annotation, OutputAnnotation<? super T, W> output,
                Iterable<Annotation> implieds) {
            checkRegistered(implieds, "Cannot imply un-registered annotation '%s'");
            register(annotation, output);
            for (Annotation implied : implieds) {
                OutputAnnotation<? super T, W> impliedOut = annotationMap.get(implied);
                implications.put(output, impliedOut);
            }
            return this;
        }

        public Builder<T, W> register(Annotation annotation, OutputAnnotation<? super T, W> output,
                Iterable<Annotation> implieds, Iterable<Annotation> overridden) {
            checkRegistered(overridden, "Cannot override un-registered annotation '%s'");
            register(annotation, output, implieds);
            overrides.putAll(output, toOutput(overridden));
            return this;
        }

        public AnnotationRegistry<T, W> build() {
            return new AnnotationRegistry<T, W>(
                    annotationMap,
                    implications.build(),
                    overrides.build(),
                    defaults.build()
            );
        }

        private Iterable<OutputAnnotation<? super T, W>> toOutput(Iterable<Annotation> annotations) {
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

    private final SetMultimap<OutputAnnotation<? super T, W>, OutputAnnotation<? super T, W>> implications;
    private final SetMultimap<OutputAnnotation<? super T, W>, OutputAnnotation<? super T, W>> overrides;
    private final BiMap<Annotation, OutputAnnotation<? super T, W>> annotationMap;
    private final Ordering<OutputAnnotation<? super T, W>> ordering;
    private final ImmutableList<OutputAnnotation<? super T, W>> defaults;

    private AnnotationRegistry(BiMap<Annotation, OutputAnnotation<? super T, W>> annotationMap,
            SetMultimap<OutputAnnotation<? super T, W>, OutputAnnotation<? super T, W>> implications,
            SetMultimap<OutputAnnotation<? super T, W>, OutputAnnotation<? super T, W>> overrides,
            ImmutableList<OutputAnnotation<? super T, W>> defaults) {
        this.annotationMap = annotationMap;
        this.implications = implications;
        this.overrides = overrides;
        this.defaults = defaults;
        this.ordering = Ordering.explicit(Annotation.all()
                .asList())
                .onResultOf(Functions.forMap(annotationMap.inverse()));
    }

    public List<OutputAnnotation<? super T, W>> activeAnnotations(Iterable<Annotation> annotations) {
        ImmutableList.Builder<OutputAnnotation<? super T, W>> writers = ImmutableList.builder();
        for (Annotation annotation : annotations) {
            OutputAnnotation<? super T, W> writer = annotationMap.get(annotation);
            if (writer != null) {
                writers.add(writer);
            }
        }
        List<OutputAnnotation<? super T, W>> flattened = flatten(writers.build());
        return ordering.immutableSortedCopy(flattened);
    }

    private List<OutputAnnotation<? super T, W>> flatten(
            Iterable<OutputAnnotation<? super T, W>> annotations) {
        Set<OutputAnnotation<? super T, W>> activeAnnotations = Sets.newHashSet();
        for (OutputAnnotation<? super T, W> annotation : annotations) {
            addWithImplied(activeAnnotations, annotation);
        }
        for (OutputAnnotation<? super T, W> annotation : annotations) {
            for (Object overridden : overrides.get(annotation)) {
                activeAnnotations.remove(overridden);
            }
        }
        return ImmutableList.copyOf(activeAnnotations);
    }

    @SuppressWarnings("unchecked")
    private void addWithImplied(Set<OutputAnnotation<? super T, W>> builder,
            OutputAnnotation<? super T, W> annotation) {
        for (OutputAnnotation<?, W> implied : implications.get(annotation)) {
            addWithImplied(builder, (OutputAnnotation<? super T, W>) implied);
        }
        builder.add(annotation);
    }

    public List<OutputAnnotation<? super T, W>> defaultAnnotations() {
        return defaults;
    }

}
