package org.atlasapi.output;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Id;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.joda.time.Duration;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ScrubbablesSegmentRelatedLinkMergerTest {

    public static final Function<RelatedLink, String> RELATED_LINK_TO_URL = new Function<RelatedLink, String>() {

        @Nullable
        @Override
        public String apply(RelatedLink input) {
            return input.getUrl();
        }
    };
    private final ScrubbablesSegmentRelatedLinkMerger merger = new ScrubbablesSegmentRelatedLinkMerger();
    private Segment selectedSegment;
    private SegmentEvent selectedSegmentEvent;
    private Multimap<Segment, SegmentEvent> segmentMap = HashMultimap.create();

    @Test
    public void testOverlappingSegmentRelatedLinksAreReturnedInSortedOrder() throws Exception {
        selectedSegment = new Segment();
        selectedSegment.setId(Id.valueOf(100L));
        selectedSegment.setDuration(Duration.standardSeconds(100L));
        selectedSegment.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "canonical"
        ).build()));

        selectedSegmentEvent = new SegmentEvent();
        selectedSegmentEvent.setOffset(Duration.standardSeconds(-20L));

        segmentMap.put(selectedSegment, selectedSegmentEvent);

        Segment tempSeg = new Segment();
        tempSeg.setDuration(Duration.standardSeconds(70L));
        tempSeg.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "notCanonicalButOverlaps"
        ).build()));

        SegmentEvent tempSegEvent = new SegmentEvent();
        tempSegEvent.setOffset(Duration.standardSeconds(-40L));

        segmentMap.put(tempSeg, tempSegEvent);

        List<RelatedLink> links = merger.getLinks(
                selectedSegment,
                selectedSegmentEvent,
                segmentMap
        );
        assertThat(links.size(), is(2));
        assertThat(
                Iterables.getFirst(links, null),
                is(selectedSegment.getRelatedLinks().iterator().next())
        );
        assertThat(Iterables.getLast(links, null), is(tempSeg.getRelatedLinks().iterator().next()));
    }

    @Test
    public void testOnlyOverlappingSegmentRelatedLinksAreReturnedInSortedOrder() throws Exception {
        selectedSegment = new Segment();
        selectedSegment.setId(Id.valueOf(100L));
        selectedSegment.setDuration(Duration.standardSeconds(100L));
        selectedSegment.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "canonical"
        ).build()));

        selectedSegmentEvent = new SegmentEvent();
        selectedSegmentEvent.setOffset(Duration.standardSeconds(-20L));

        segmentMap.put(selectedSegment, selectedSegmentEvent);

        Segment tempSeg = new Segment();
        SegmentEvent tempSegEvent = new SegmentEvent();

        tempSeg.setDuration(Duration.standardSeconds(1L));
        tempSeg.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "notCanonicalAndDoesntOverlap"
        ).build()));
        tempSegEvent.setOffset(Duration.standardSeconds(-100L));
        segmentMap.put(tempSeg, tempSegEvent);

        tempSeg = new Segment();
        tempSegEvent = new SegmentEvent();
        tempSeg.setDuration(Duration.standardSeconds(70L));
        tempSeg.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "notCanonicalButOverlaps"
        ).build()));
        tempSegEvent.setOffset(Duration.standardSeconds(-40L));
        segmentMap.put(tempSeg, tempSegEvent);

        List<RelatedLink> links = merger.getLinks(
                selectedSegment,
                selectedSegmentEvent,
                segmentMap
        );
        assertThat(links.size(), is(2));
        assertThat(
                Iterables.getFirst(links, null),
                is(selectedSegment.getRelatedLinks().iterator().next())
        );
        assertThat(Iterables.getLast(links, null), is(tempSeg.getRelatedLinks().iterator().next()));
    }

    @Test
    public void testSortedOrderRelatedLinks() throws Exception {
        selectedSegment = new Segment();
        selectedSegment.setId(Id.valueOf(1L));
        selectedSegment.setDuration(Duration.standardSeconds(60L));
        selectedSegment.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "full"
        ).build()));

        selectedSegmentEvent = new SegmentEvent();
        selectedSegmentEvent.setOffset(Duration.standardSeconds(0L));
        segmentMap.put(selectedSegment, selectedSegmentEvent);

        Segment tempSeg = new Segment();
        SegmentEvent tempSegEvent = new SegmentEvent();

        /* 30s overlap */
        tempSeg.setId(Id.valueOf(30L));
        tempSeg.setDuration(Duration.standardSeconds(30L));
        tempSeg.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "30"
        ).build()));
        tempSegEvent.setOffset(Duration.standardSeconds(10L));
        segmentMap.put(tempSeg, tempSegEvent);

        tempSeg = new Segment();
        tempSegEvent = new SegmentEvent();
        /* 0 overlap*/
        tempSeg.setId(Id.valueOf(0L));
        tempSeg.setDuration(Duration.standardSeconds(1L));
        tempSeg.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "0"
        ).build()));
        tempSegEvent.setOffset(Duration.standardSeconds(-100L));
        segmentMap.put(tempSeg, tempSegEvent);

        tempSeg = new Segment();
        tempSegEvent = new SegmentEvent();
        /* 10 overlap */
        tempSeg.setId(Id.valueOf(10L));
        tempSeg.setDuration(Duration.standardSeconds(10l));
        tempSeg.setRelatedLinks(ImmutableList.of(RelatedLink.relatedLink(
                RelatedLink.LinkType.ARTICLE,
                "10"
        ).build()));
        tempSegEvent.setOffset(Duration.standardSeconds(50L));
        segmentMap.put(tempSeg, tempSegEvent);

        tempSeg = new Segment();
        tempSegEvent = new SegmentEvent();
        /* 15 overlap */
        tempSeg.setId(Id.valueOf(15L));
        tempSeg.setDuration(Duration.standardSeconds(15L));
        tempSeg.setRelatedLinks(
                ImmutableList.of(
                        RelatedLink.relatedLink(RelatedLink.LinkType.ARTICLE, "15A").build(),
                        RelatedLink.relatedLink(RelatedLink.LinkType.ARTICLE, "15B").build()
                )
        );
        tempSegEvent.setOffset(Duration.standardSeconds(40L));
        segmentMap.put(tempSeg, tempSegEvent);

        tempSeg = new Segment();
        tempSegEvent = new SegmentEvent();
        /* 5 overlap */
        tempSeg.setId(Id.valueOf(5L));
        tempSeg.setDuration(Duration.standardSeconds(5l));
        tempSeg.setRelatedLinks(
                ImmutableList.of(
                        RelatedLink.relatedLink(RelatedLink.LinkType.ARTICLE, "5A").build(),
                        RelatedLink.relatedLink(RelatedLink.LinkType.ARTICLE, "5B").build()
                )
        );
        tempSegEvent.setOffset(Duration.standardSeconds(20l));
        segmentMap.put(tempSeg, tempSegEvent);

        tempSeg = new Segment();
        tempSegEvent = new SegmentEvent();
        /* 20 overlap */
        tempSeg.setId(Id.valueOf(20L));
        tempSeg.setDuration(Duration.standardSeconds(20L));
        tempSeg.setRelatedLinks(ImmutableList.<RelatedLink>of());
        tempSegEvent.setOffset(Duration.standardSeconds(0l));
        segmentMap.put(tempSeg, tempSegEvent);

        List<RelatedLink> links = merger.getLinks(
                selectedSegment,
                selectedSegmentEvent,
                segmentMap
        );
        assertThat(links.size(), is(7));
        assertThat(
                ImmutableList.copyOf(Iterables.transform(links, RELATED_LINK_TO_URL)),
                is(ImmutableList.of("full", "30", "15A", "15B", "10", "5A", "5B"))
        );
    }
}