package org.atlasapi.system.legacy;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.atlasapi.content.Content;
import org.atlasapi.content.Tag;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.TopicRef;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LegacyContentGenresTransformerTest {

    @Mock
    private ChannelResolver channelResolver;

    @Mock
    private LegacySegmentMigrator legacySegmentMigrator;

    @Mock
    private GenreToTagMapper genreToTagMapper;

    @InjectMocks
    LegacyContentTransformer contentTransformerUnderTest;

    @Test
    public void mergeTagsOnContentTransformer() {

        LegacyContentTopicMerger merger = new LegacyContentTopicMerger();

        List<TopicRef> left = Lists.newArrayList(
                new TopicRef(1L, 0.0f, false, TopicRef.Relationship.ABOUT),
                new TopicRef(2L, 0.0f, false, TopicRef.Relationship.ABOUT)
        );

        List<TopicRef> right = Lists.newArrayList(
                new TopicRef(2L, 3.0f, false, TopicRef.Relationship.ABOUT),
                new TopicRef(3L, 3.0f, false, TopicRef.Relationship.ABOUT)
        );

        List<TopicRef> expected = Lists.newArrayList(
                new TopicRef(3L, 3.0f, false, TopicRef.Relationship.ABOUT),
                new TopicRef(2L, 0.0f, false, TopicRef.Relationship.ABOUT),
                new TopicRef(1L, 0.0f, false, TopicRef.Relationship.ABOUT)
        );

        List<TopicRef> output = merger.mergeTags(left, right);

        assertThat(output.size(), is(expected.size()));
        assertThat(output.containsAll(expected), is(true));
    }

    @Test
    public void legacyContentTransformerNullGenreMapper() {
        when(genreToTagMapper.mapGenresToTopicRefs((Set<String>) notNull())).thenReturn(Collections.emptySet());

        Item contentToTransform = new Item();
        contentToTransform.setId(666L);
        contentTransformerUnderTest.apply(contentToTransform);

        verify(genreToTagMapper, times(1)).mapGenresToTopicRefs(any(Set.class));
        verifyNoMoreInteractions(genreToTagMapper);
    }

    @Test
    public void legacyContentTransformerStubGenreMapper() {
        Set<TopicRef> genreMap = ImmutableSet.of(
                new TopicRef(1L, 0.0f, false, TopicRef.Relationship.ABOUT),
                new TopicRef(2L, 0.0f, false, TopicRef.Relationship.ABOUT),
                new TopicRef(3L, 0.0f, false, TopicRef.Relationship.ABOUT)
        );

        when(genreToTagMapper.mapGenresToTopicRefs((Set<String>) notNull())).thenReturn(genreMap);


        Set<TopicRef> inputTopics = ImmutableSet.of(
                new TopicRef(4L, 0.0f, false, TopicRef.Relationship.ABOUT),
                new TopicRef(5L, 0.0f, false, TopicRef.Relationship.ABOUT)
        );

        Item contentToTransform = new Item();
        contentToTransform.setId(666L);
        contentToTransform.setTopicRefs(inputTopics);

        Content transformed = contentTransformerUnderTest.apply(contentToTransform);

        assertThat(transformed.getTags().size(), is(genreMap.size() + inputTopics.size()));
        assertThat(transformed.getTags(), containsInAnyOrder(
                new Tag(1L, 0.0f, false, Tag.Relationship.ABOUT),
                new Tag(2L, 0.0f, false, Tag.Relationship.ABOUT),
                new Tag(3L, 0.0f, false, Tag.Relationship.ABOUT),
                new Tag(4L, 0.0f, false, Tag.Relationship.ABOUT),
                new Tag(5L, 0.0f, false, Tag.Relationship.ABOUT)
        ));

        verify(genreToTagMapper, times(1)).mapGenresToTopicRefs(any(Set.class));
        verifyNoMoreInteractions(genreToTagMapper);
    }
}
