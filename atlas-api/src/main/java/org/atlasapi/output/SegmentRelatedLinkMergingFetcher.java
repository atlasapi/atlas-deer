package org.atlasapi.output;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
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

    public SegmentRelatedLinkMergingFetcher(SegmentResolver segmentResolver, ScrubbablesSegmentRelatedLinkMerger segmentRelatedLinkMerger) {
        this.segmentResolver = checkNotNull(segmentResolver);
        this.segmentRelatedLinkMerger = checkNotNull(segmentRelatedLinkMerger);
    }

    /**
     * Returns a {@link org.atlasapi.output.SegmentAndEventTuple} containing the first SegmentEvent
     * that exists on the Item passed in, and the Segment it references. With the overlapping RelatedLinks
     * from all other Segments referenced by other SegmentEvents merged in and in descending order of their
     * overlap duration.
     *
     * @param segmentEvents - List of SegmentEvents - the first of which will be used as the reference point for comparing
     *             and merging all others.
     * @return A tuple containing the SegmentEvent and referenced SegmentEvent, with all RelatedLinks
     * from other merged Segments into the contained Segment. Or null if the list was empty.
     */
    public Iterable<SegmentAndEventTuple> mergeSegmentLinks(List<SegmentEvent> segmentEvents) {
        if (segmentEvents.isEmpty()) {
            return null;
        }
        final Publisher chosenPublisher = Iterables.getFirst(segmentEvents, null).getSource();
        Iterable<SegmentEvent> publisherSegments = Iterables.filter(segmentEvents, new Predicate<SegmentEvent>() {
            @Override
            public boolean apply(SegmentEvent input) {
                return input.getSource().equals(chosenPublisher);
            }
        });

        Iterable<SegmentEvent> otherPublisherSegments = Iterables.filter(segmentEvents, new Predicate<SegmentEvent>() {
            @Override
            public boolean apply(SegmentEvent input) {
                return !input.getSource().equals(chosenPublisher);
            }
        });

        ImmutableList.Builder<SegmentAndEventTuple> segmentAndEvents = ImmutableList.builder();
        ImmutableMultimap<Segment, SegmentEvent> segmentMap = resolveSegments(segmentEvents);
        for (SegmentEvent selectedSegmentEvent : publisherSegments) {
            Segment selectedSegment = Iterables.getOnlyElement(segmentMap.inverse().get(selectedSegmentEvent));
            ImmutableList.Builder<SegmentEvent> segmentsToResolve = ImmutableList.builder();
            segmentsToResolve.add(selectedSegmentEvent);
            segmentsToResolve.addAll(otherPublisherSegments);
            selectedSegment.setRelatedLinks(
                    segmentRelatedLinkMerger.getLinks(
                            selectedSegment,
                            selectedSegmentEvent,
                            resolveSegments(segmentsToResolve.build())
                    )
            );
            segmentAndEvents.add(new SegmentAndEventTuple(selectedSegment, selectedSegmentEvent));

        }
        return segmentAndEvents.build();

    }

    private ImmutableMultimap<Segment, SegmentEvent> resolveSegments(final Iterable<SegmentEvent> segmentEvents) {
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
