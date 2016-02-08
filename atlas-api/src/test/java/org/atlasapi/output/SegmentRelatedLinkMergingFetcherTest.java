package org.atlasapi.output;

import org.atlasapi.content.Item;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.atlasapi.segment.SegmentResolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.junit.Test;
import org.mockito.Mock;

import static org.atlasapi.content.RelatedLink.LinkType.ARTICLE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentRelatedLinkMergingFetcherTest {

    private @Mock SegmentResolver segmentResolver = mock(SegmentResolver.class);
    private final ScrubbablesSegmentRelatedLinkMerger segmentRelatedLinkMerger =
            new ScrubbablesSegmentRelatedLinkMerger();
    private final SegmentRelatedLinkMergingFetcher linkMergingFetcher =
            new SegmentRelatedLinkMergingFetcher(segmentResolver, segmentRelatedLinkMerger);

    private Segment segmentOne() {
        Segment segment = new Segment();
        segment.setId(1L);
        segment.setDuration(Duration.standardSeconds(10l));
        segment.setRelatedLinks(ImmutableList.of(
                RelatedLink.relatedLink(ARTICLE, "1A").build(),
                RelatedLink.relatedLink(ARTICLE, "1B").build()
        ));
        return segment;
    }

    private Segment segmentTwo() {
        Segment segment = new Segment();
        segment.setId(2L);
        segment.setDuration(Duration.standardSeconds(10l));
        segment.setRelatedLinks(ImmutableList.of(
                RelatedLink.relatedLink(ARTICLE, "2A").build(),
                RelatedLink.relatedLink(ARTICLE, "2B").build()
        ));
        return segment;
    }

    private Segment segmentThree() {
        Segment segment = new Segment();
        segment.setId(3L);
        segment.setDuration(Duration.standardSeconds(10l));
        segment.setRelatedLinks(ImmutableList.of(
                RelatedLink.relatedLink(ARTICLE, "3A").build(),
                RelatedLink.relatedLink(ARTICLE, "3B").build()
        ));
        return segment;
    }

    private Segment segmentFour() {
        Segment segment = new Segment();
        segment.setId(4L);
        segment.setDuration(Duration.standardSeconds(90l));
        segment.setRelatedLinks(ImmutableList.of(
                RelatedLink.relatedLink(ARTICLE, "4A").build(),
                RelatedLink.relatedLink(ARTICLE, "4B").build()
        ));
        return segment;
    }

    @Test
    public void testMergeSegmentLinks() throws Exception {
        when(segmentResolver.resolveSegments(
                ImmutableSet.of(
                        Id.valueOf(2L),
                        Id.valueOf(3L),
                        Id.valueOf(4L)
                )
        )).thenReturn(ImmutableList.of(segmentTwo(), segmentThree(), segmentFour()));
        when(segmentResolver.resolveSegments(
                ImmutableSet.of(
                        Id.valueOf(1)
                )
        )).thenReturn(ImmutableList.of(segmentOne()));
        when(segmentResolver.resolveSegments(
                ImmutableSet.of(
                        Id.valueOf(1L),
                        Id.valueOf(2L),
                        Id.valueOf(3L),
                        Id.valueOf(4L)
                )
        )).thenReturn(ImmutableList.of(segmentOne(), segmentTwo(), segmentThree(), segmentFour()));

        Item item = new Item();
        item.setSegmentEvents(segmentEvents());
        Iterable<SegmentAndEventTuple> segmentAndEventTuple = linkMergingFetcher.mergeSegmentLinks(
                item.getSegmentEvents());

        assertThat(Iterables.getOnlyElement(segmentAndEventTuple)
                .getSegment()
                .getRelatedLinks()
                .size(), is(6));
        assertThat(Sets.intersection(
                segmentFour().getRelatedLinks(),
                Iterables.getOnlyElement(segmentAndEventTuple).getSegment().getRelatedLinks()
        ).size(), is(0));
    }

    private ImmutableList<SegmentEvent> segmentEvents() {

        SegmentEvent seOne = new SegmentEvent();
        seOne.setId(Id.valueOf(10L));
        seOne.setSegment(new SegmentRef(Id.valueOf(1l), Publisher.BBC));
        seOne.setOffset(Duration.standardSeconds(0l));
        seOne.setPublisher(Publisher.BBC);

        SegmentEvent seTwo = new SegmentEvent();
        seTwo.setId(Id.valueOf(20L));
        seTwo.setSegment(new SegmentRef(Id.valueOf(2l), Publisher.BBC));
        seTwo.setOffset(Duration.standardSeconds(0l));
        seTwo.setPublisher(Publisher.PA);

        SegmentEvent seThree = new SegmentEvent();
        seThree.setId(Id.valueOf(30L));
        seThree.setSegment(new SegmentRef(Id.valueOf(3l), Publisher.BBC));
        seThree.setOffset(Duration.standardSeconds(0l));
        seThree.setPublisher(Publisher.PA);

        SegmentEvent seFour = new SegmentEvent();
        seFour.setId(Id.valueOf(40L));
        seFour.setSegment(new SegmentRef(Id.valueOf(4l), Publisher.BBC));
        seFour.setOffset(Duration.standardSeconds(200l));
        seFour.setPublisher(Publisher.PA);

        return ImmutableList.of(seOne, seTwo, seThree, seFour);
    }
}