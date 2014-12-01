package org.atlasapi.output;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;

public class SegmentRelatedLinkMergingFetcher {

    public static final Function<Segment, Id> SEGMENT_TO_ID = new Function<Segment, Id>() {

        @Override
        public Id apply(Segment input) {
            return input.getId();
        }
    };
    public static final Function<SegmentEvent, Id> SEG_EVENT_TO_SEG_ID = new Function<SegmentEvent, Id>() {

        @Override
        public Id apply(SegmentEvent input) {
            return input.getSegmentRef().getId();
        }
    };
    private final SegmentRelatedLinkMerger segmentRelatedLinkMerger;
    private final SegmentResolver segmentResolver;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public SegmentRelatedLinkMergingFetcher(SegmentResolver segmentResolver, SegmentRelatedLinkMerger segmentRelatedLinkMerger) {
        this.segmentResolver = checkNotNull(segmentResolver);
        this.segmentRelatedLinkMerger = checkNotNull(segmentRelatedLinkMerger);
    }

    public SegmentAndEventTuple mergeSegmentLinks(Item item) {
        List<SegmentEvent> segmentEvents = item.getSegmentEvents();
        ImmutableMultimap<Segment, SegmentEvent> segmentMap = resolveSegments(segmentEvents);
        SegmentEvent selectedSegmentEvent = Iterables.getFirst(segmentEvents, null);
        Segment selectedSegment = Iterables.getOnlyElement(segmentMap.inverse().get(selectedSegmentEvent));

        if (selectedSegment == null) {
            log.warn("Failed to resolve selected segment {}",
                    Iterables.getOnlyElement(segmentMap.inverse().get(segmentEvents.get(0))).getId()
            );
            return new SegmentAndEventTuple(null, selectedSegmentEvent);
        }

        selectedSegment.setRelatedLinks(segmentRelatedLinkMerger.getLinks(
                selectedSegment, selectedSegmentEvent, resolveSegments(segmentEvents)
        ));
        return new SegmentAndEventTuple(selectedSegment, selectedSegmentEvent);
    }

    private ImmutableMultimap<Segment, SegmentEvent> resolveSegments(final List<SegmentEvent> segmentEvents) {
        final ImmutableListMultimap<Id, SegmentEvent> segmentEventToSegmentIds = Multimaps.index(
                segmentEvents,
                SEG_EVENT_TO_SEG_ID
        );
        final ImmutableMap<Id, Segment> segmentsToIds = Maps.uniqueIndex(
                segmentResolver.resolveSegments(segmentEventToSegmentIds.keySet()),
                SEGMENT_TO_ID
        );

        ImmutableMultimap.Builder<Segment, SegmentEvent> segmentMappngs = ImmutableMultimap.builder();
        for (Map.Entry<Id, SegmentEvent> entry : segmentEventToSegmentIds.entries()) {
            segmentMappngs.put(segmentsToIds.get(entry.getKey()), entry.getValue());
        }
        return segmentMappngs.build();
    }
}
