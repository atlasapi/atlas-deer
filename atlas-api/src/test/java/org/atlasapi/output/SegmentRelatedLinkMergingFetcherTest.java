package org.atlasapi.output;

import static org.atlasapi.content.RelatedLink.LinkType.ARTICLE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;



import org.atlasapi.content.Item;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.atlasapi.segment.SegmentResolver;
import org.joda.time.Duration;
import org.junit.Test;
import org.mockito.Mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class SegmentRelatedLinkMergingFetcherTest {

    private @Mock SegmentResolver segmentResolver = mock(SegmentResolver.class);
    private final SegmentRelatedLinkMerger segmentRelatedLinkMerger =
            new SegmentRelatedLinkMerger();
    private final SegmentRelatedLinkMergingFetcher linkMergingFetcher =
            new SegmentRelatedLinkMergingFetcher(segmentResolver, segmentRelatedLinkMerger);
    private final Segment seg1 = segmentOne();

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
                        Id.valueOf(1L),
                        Id.valueOf(2L),
                        Id.valueOf(3L),
                        Id.valueOf(4L)
                )
        )).thenReturn(ImmutableList.of(segmentOne(), segmentTwo(), segmentThree(), segmentFour()));

        Item item = new Item();
        item.setSegmentEvents(segmentEvents());
        SegmentAndEventTuple segmentAndEventTuple = linkMergingFetcher.mergeSegmentLinks(item);

        assertTrue(segmentAndEventTuple.getSegment().getRelatedLinks().size() == 6);
        assertTrue(Sets.intersection(segmentFour().getRelatedLinks(), segmentAndEventTuple.getSegment().getRelatedLinks()).isEmpty());
    }

    private ImmutableList<SegmentEvent> segmentEvents() {

        SegmentEvent seOne = new SegmentEvent();
        seOne.setId(Id.valueOf(10L));
        seOne.setSegment(new SegmentRef(Id.valueOf(1l), Publisher.BBC));
        seOne.setOffset(Duration.standardSeconds(0l));

        SegmentEvent seTwo = new SegmentEvent();
        seTwo.setId(Id.valueOf(20L));
        seTwo.setSegment(new SegmentRef(Id.valueOf(2l), Publisher.BBC));
        seTwo.setOffset(Duration.standardSeconds(0l));

        SegmentEvent seThree = new SegmentEvent();
        seThree.setId(Id.valueOf(30L));
        seThree.setSegment(new SegmentRef(Id.valueOf(3l), Publisher.BBC));
        seThree.setOffset(Duration.standardSeconds(0l));

        SegmentEvent seFour = new SegmentEvent();
        seFour.setId(Id.valueOf(40L));
        seFour.setSegment(new SegmentRef(Id.valueOf(4l), Publisher.BBC));
        seFour.setOffset(Duration.standardSeconds(200l));
        return ImmutableList.of(seOne, seTwo, seThree, seFour);
    }
}