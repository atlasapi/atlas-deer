package org.atlasapi.query.annotation;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.exceptions.InvalidAnnotationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p> A set of bindings for resolving requested annotation keys to their related {@link
 * PathAnnotation} values. <p>
 * <p>
 * <p> In a standard, single resource {@code ResourceAnnotationIndex} values can be resolved with or
 * without their overarching resource context. That is, for a {@link Resource}.CONTENT index {@code
 * 'description'} and {@code 'content.description'} both resolveContent {@code ([CONTENT] -> DESCRIPTION)}
 * . </p>
 * <p>
 * <p> Indices can be attached to others so {@link Annotation}s for a resource type apply where that
 * type appears within the model of another. The {@code Resource.TOPIC} index can be attached to the
 * {@code Resource.CONTENT} index given an attachment {@code Annotation}, e.g. {@code TAGS}, and an
 * implicitly-activated {@code Annotation} in the attached index, e.g. {@code ID}. </p>
 * <p>
 * <p> {@code 'topics'} or {@code 'content.topics'} : {@code [([CONTENT] -> TAGS), ([CONTENT, TOPIC]
 * -> ID)]} </p> <p> {@code 'topics.topic.description'} or {@code 'content.topics.topic.description'}
 * : {@code [([CONTENT] -> TAGS), ([CONTENT, TOPIC] -> DESCRIPTION)]} </p>
 * <p>
 * <p> Indices can be combined for multiple top-level resource types. Context can be explicitly
 * required for any of the combined indices. </p>
 */
public class ResourceAnnotationIndex implements AnnotationIndex {

    public static final IndexCombination combination() {
        return new IndexCombination();
    }

    public static final Builder builder(Resource resource, Iterable<Annotation> annotations) {
        return new Builder(resource, annotations);
    }

    public static final class Builder {

        private final Resource resource;
        private final ImmutableSet<Annotation> annotations;
        private final Map<Annotation, ResourceAnnotationIndex> attached = Maps.newHashMap();
        private final Map<Annotation, Annotation> defaultImplicit = Maps.newHashMap();

        public Builder(Resource resource, Iterable<Annotation> annotations) {
            this.resource = checkNotNull(resource);
            this.annotations = ImmutableSet.copyOf(annotations);
        }

        public Builder attach(Annotation attachment, ResourceAnnotationIndex index,
                Annotation implicit) {
            attached.put(checkNotNull(attachment), checkNotNull(index));
            defaultImplicit.put(attachment, checkNotNull(implicit));
            return this;
        }

        public ResourceAnnotationIndex build() {
            return new ResourceAnnotationIndex(resource,
                    buildBindings(resource.getSingular()), buildBindings(resource.getPlural())
            );
        }

        private Multimap<String, PathAnnotation> buildBindings(String contextPrefix) {
            ImmutableMultimap.Builder<String, PathAnnotation> builder = ImmutableMultimap.builder();
            for (Annotation annotation : annotations) {
                PathAnnotation pathAnnotation = new PathAnnotation(
                        ImmutableList.of(resource),
                        annotation
                );

                builder.put(join(contextPrefix, annotation.toKey()), pathAnnotation);
                builder.put(annotation.toKey(), pathAnnotation);

                if (attached.containsKey(annotation)) {
                    builder.putAll(attachedBindings(contextPrefix,
                            annotation, attached.get(annotation),
                            defaultImplicit.get(annotation)
                    ));
                }
            }

            return builder.build();
        }

        private ImmutableMultimap<String, PathAnnotation> attachedBindings(String contextPrefix,
                Annotation annotation, ResourceAnnotationIndex attachedIndex, Annotation implied) {
            ImmutableMultimap.Builder<String, PathAnnotation> attachedBindings = ImmutableMultimap.builder();
            Multimap<String, PathAnnotation> explicitBindings = explicitBindings(attachedIndex);

            String contextlessKey = annotation.toKey();
            String contextKey = join(contextPrefix, contextlessKey);

            for (Entry<String, PathAnnotation> atBinding : explicitBindings.entries()) {
                String atKey = atBinding.getKey();
                PathAnnotation atPathAnnotation = atBinding.getValue();
                PathAnnotation joinedAnnotation = new PathAnnotation(
                        prefixPath(resource, atPathAnnotation.getPath()),
                        atPathAnnotation.getAnnotation()
                );

                if (implied.equals(atPathAnnotation.getAnnotation())) {
                    attachedBindings.put(contextKey, joinedAnnotation);
                    attachedBindings.put(contextlessKey, joinedAnnotation);
                }

                PathAnnotation pathAnnotation = new PathAnnotation(
                        ImmutableList.of(resource),
                        annotation
                );
                attachedBindings.putAll(
                        join(contextlessKey, atKey),
                        joinedAnnotation,
                        pathAnnotation
                );
                attachedBindings.putAll(join(contextKey, atKey), joinedAnnotation, pathAnnotation);
            }
            return attachedBindings.build();
        }

        private Multimap<String, PathAnnotation> explicitBindings(
                ResourceAnnotationIndex index) {
            return index.singleIndex.filterBindings(index.resource.getSingular());
        }

        private List<Resource> prefixPath(Resource prefix, List<Resource> path) {
            return ImmutableList.<Resource>builder()
                    .add(resource)
                    .addAll(path)
                    .build();
        }

        private String join(String prefix, String suffix) {
            return String.format("%s.%s", prefix, suffix);
        }

    }

    final Resource resource;
    final Index singleIndex;
    final Index listIndex;

    public ResourceAnnotationIndex(Resource resource,
            Multimap<String, PathAnnotation> singleBindings,
            Multimap<String, PathAnnotation> listBindings) {
        this.resource = resource;
        this.singleIndex = Index.create(singleBindings);
        this.listIndex = Index.create(listBindings);
    }

    @Override
    public ActiveAnnotations resolveListContext(Iterable<String> keys)
            throws InvalidAnnotationException {
        return listIndex.resolve(keys);
    }

    @Override
    public ActiveAnnotations resolveSingleContext(Iterable<String> keys)
            throws InvalidAnnotationException {
        return singleIndex.resolve(keys);
    }
}
