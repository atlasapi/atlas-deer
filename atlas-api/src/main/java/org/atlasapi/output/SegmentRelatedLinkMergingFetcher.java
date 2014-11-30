package org.atlasapi.output;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.content.Item;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Id;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;

public class SegmentRelatedLinkMergingFetcher {

    private final SegmentRelatedLinkMerger segmentRelatedLinkMerger;
    private final SegmentResolver segmentResolver;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public SegmentRelatedLinkMergingFetcher(SegmentResolver segmentResolver, SegmentRelatedLinkMerger segmentRelatedLinkMerger) {
        this.segmentResolver = checkNotNull(segmentResolver);
        this.segmentRelatedLinkMerger = checkNotNull(segmentRelatedLinkMerger);
    }

    public ImmutableList<RelatedLink> fetchAndMergeRelatedLinks(Item item) {
        List<SegmentEvent> segmentEvents = item.getSegmentEvents();
        ImmutableMultimap<Segment, SegmentEvent> segmentMap = resolveSegments(segmentEvents);
        SegmentEvent selectedSegmentEvent = Iterables.getFirst(segmentEvents, null);
        Segment selectedSegment = Iterables.getOnlyElement(segmentMap.inverse().get(selectedSegmentEvent));

        if (selectedSegment == null) {
            log.warn("Failed to resolve canonical segment {}",
                    Iterables.getOnlyElement(segmentMap.inverse().get(segmentEvents.get(0))).getId()
            );
            return ImmutableList.of();
        }

        return segmentRelatedLinkMerger.getLinks(
                selectedSegment, selectedSegmentEvent, resolveSegments(segmentEvents)
        );
    }

    private ImmutableMultimap<Segment, SegmentEvent> resolveSegments(List<SegmentEvent> segmentEvents) {
        ImmutableListMultimap<Id, SegmentEvent> segmentEventToSegmentIds = Multimaps.index(segmentEvents, new Function<SegmentEvent, Id>() {
            @Nullable
            @Override
            public Id apply(@Nullable SegmentEvent input) {
                return input.getId();
            }
        });

        final ImmutableMap<Id, Segment> segmentsToIds = Maps.uniqueIndex(segmentResolver.resolveSegments(segmentEventToSegmentIds.keySet()), new Function<Segment, Id>() {
            @Nullable
            @Override
            public Id apply(@Nullable Segment input) {
                return input.getId();
            }
        });

        ImmutableMultimap.Builder<Segment, SegmentEvent> segmentMappngs = ImmutableMultimap.builder();
        for (Map.Entry<Id, SegmentEvent> entry : segmentEventToSegmentIds.entries()) {
            segmentMappngs.put(segmentsToIds.get(entry.getKey()), entry.getValue());
        }
        return segmentMappngs.build();
    }
}
