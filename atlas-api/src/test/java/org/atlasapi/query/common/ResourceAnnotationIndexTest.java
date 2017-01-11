package org.atlasapi.query.common;

import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.annotation.ContextualAnnotationIndex;
import org.atlasapi.query.annotation.ResourceAnnotationIndex;
import org.atlasapi.query.common.exceptions.InvalidAnnotationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ResourceAnnotationIndexTest {

    private final Set<Annotation> annotations
            = ImmutableSet.of(Annotation.ID, Annotation.TAGS, Annotation.EXTENDED_DESCRIPTION);

    private final ResourceAnnotationIndex topicIndex
            = ResourceAnnotationIndex.builder(Resource.TOPIC, annotations).build();
    private final ResourceAnnotationIndex channelIndex
            = ResourceAnnotationIndex.builder(Resource.CHANNEL, annotations)
            .attach(Annotation.TAGS, topicIndex, Annotation.ID)
            .build();

    private final ContextualAnnotationIndex combinedIndex
            = ResourceAnnotationIndex.combination()
            .addExplicitSingleContext(topicIndex)
            .addImplicitListContext(channelIndex)
            .combine();

    @Test
    public void testListLookupTopLevelContextless() throws Exception {
        check(channelIndex.resolveListContext(ImmutableList.of("extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testListLookupTopLevelContext() throws Exception {
        check(channelIndex.resolveListContext(ImmutableList.of("channels.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testListLookupAttachedTopContextless() throws Exception {
        check(channelIndex.resolveListContext(ImmutableList.of("tags")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.ID
        );
    }

    @Test
    public void testListLookupAttachedTopContext() throws Exception {
        check(channelIndex.resolveListContext(ImmutableList.of("channels.tags")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.ID
        );
    }

    @Test
    public void testListLookupAttachedSubContextless() throws Exception {
        check(channelIndex.resolveListContext(ImmutableList.of("tags.topic.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testListLookupAttachedSubContext() throws Exception {
        check(channelIndex.resolveListContext(ImmutableList.of(
                "channels.tags.topic.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testSingleLookupTopLevelContextless() throws Exception {
        check(channelIndex.resolveSingleContext(ImmutableList.of("extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testSingleLookupTopLevelContext() throws Exception {
        check(channelIndex.resolveSingleContext(ImmutableList.of("channel.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testSingleLookupAttachedTopContextless() throws Exception {
        check(channelIndex.resolveSingleContext(ImmutableList.of("tags")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.ID
        );
    }

    @Test
    public void testSingleLookupAttachedTopContext() throws Exception {
        check(channelIndex.resolveSingleContext(ImmutableList.of("channel.tags")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.ID
        );
    }

    @Test
    public void testSingleLookupAttachedSubContextless() throws Exception {
        check(channelIndex.resolveSingleContext(ImmutableList.of("tags.topic.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testSingleLookupAttachedSubContext() throws Exception {
        check(channelIndex.resolveSingleContext(ImmutableList.of(
                "channel.tags.topic.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testSingleLookupMixed() throws Exception {
        check(channelIndex.resolveSingleContext(ImmutableList.of("channel.tags",
                "extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS, Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testListLookupMixed() throws Exception {
        check(channelIndex.resolveListContext(ImmutableList.of("channels.tags",
                "extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS, Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test(expected = InvalidAnnotationException.class)
    public void testSingleLookupInvalidForPlural() throws Exception {
        channelIndex.resolveSingleContext(ImmutableList.of("channels.extended_description"));
    }

    @Test(expected = InvalidAnnotationException.class)
    public void testListLookupInvalidForSingular() throws Exception {
        channelIndex.resolveListContext(ImmutableList.of("channel.extended_description"));
    }

    @Test(expected = InvalidAnnotationException.class)
    public void testListLookupInvalidForBadPath() throws Exception {
        channelIndex.resolveListContext(ImmutableList.of("topics.description"));
    }

    @Test
    public void testCombinedLookupContextless() throws Exception {
        check(combinedIndex.resolve(ImmutableList.of("extended_description",
                "topic.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.EXTENDED_DESCRIPTION,
                ImmutableList.of(Resource.TOPIC), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testCombinedLookupContext() throws Exception {
        check(combinedIndex.resolve(ImmutableList.of("channels.extended_description",
                "topic.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.EXTENDED_DESCRIPTION,
                ImmutableList.of(Resource.TOPIC), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testCombinedLookupAttachedTopContextless() throws Exception {
        check(combinedIndex.resolve(ImmutableList.of("tags")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.ID
        );
    }

    @Test
    public void testCombinedLookupAttachedTopContext() throws Exception {
        check(combinedIndex.resolve(ImmutableList.of("channels.tags")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.ID
        );
    }

    @Test
    public void testCombinedLookupAttachedSubContextless() throws Exception {
        check(combinedIndex.resolve(ImmutableList.of("tags.topic.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test
    public void testCombinedLookupAttachedSubContext() throws Exception {
        check(combinedIndex.resolve(ImmutableList.of("channels.tags.topic.extended_description")),
                ImmutableList.of(Resource.CHANNEL), Annotation.TAGS,
                ImmutableList.of(Resource.CHANNEL, Resource.TOPIC), Annotation.EXTENDED_DESCRIPTION
        );
    }

    @Test(expected = InvalidAnnotationException.class)
    public void testCombinedLookupInvalidForBadPath() throws Exception {
        ResourceAnnotationIndex topicIndex = ResourceAnnotationIndex.builder(
                Resource.TOPIC,
                annotations
        ).build();
        ResourceAnnotationIndex contentIndex = ResourceAnnotationIndex.builder(
                Resource.CONTENT,
                annotations
        )
                .attach(Annotation.TAGS, topicIndex, Annotation.ID)
                .build();
        ResourceAnnotationIndex.combination()
                .addImplicitListContext(contentIndex)
                .addExplicitSingleContext(topicIndex)
                .combine()
                .resolve(ImmutableList.of("topics.extended_description"));
    }

    private void check(ActiveAnnotations actual, ImmutableList<Resource> path,
            Annotation... annotations) {
        assertThat(
                String.format("%s -> %s", path, annotations),
                actual.forPath(path),
                is(ImmutableSet.copyOf(annotations))
        );
    }

    private void check(ActiveAnnotations actual, ImmutableList<Resource> path1,
            Annotation annotation1, ImmutableList<Resource> path2, Annotation annotation2) {
        check(actual, path1, annotation1);
        check(actual, path2, annotation2);
    }

}
