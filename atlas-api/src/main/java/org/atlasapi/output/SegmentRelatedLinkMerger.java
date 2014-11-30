package org.atlasapi.output;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.content.RelatedLink;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

public class SegmentRelatedLinkMerger {

    public static final Function<Map.Entry<Segment, SegmentEvent>, Set<RelatedLink>> PROJECT_RELATED_LINKS =
            new Function<Map.Entry<Segment, SegmentEvent>, Set<RelatedLink>>() {
        @Nullable
        @Override
        public Set<RelatedLink> apply(Map.Entry<Segment, SegmentEvent> input) {
            return input.getKey().getRelatedLinks();
        }
    };

    public ImmutableList<RelatedLink> getLinks(Segment selectedSegment, SegmentEvent selectedSegmentEvent,
                                               Multimap<Segment, SegmentEvent> segmentMap) {

        Interval selectedInterval = intervalFrom(selectedSegment, selectedSegmentEvent);
        ImmutableMultimap<Segment, SegmentEvent> filtered = filterNonOverlapping(selectedInterval, segmentMap);
        return FluentIterable.from(
                intervalOrdering(selectedInterval).immutableSortedCopy(filtered.entries())
        ).transformAndConcat(PROJECT_RELATED_LINKS).toList();

    }

    private Ordering<Map.Entry<Segment, SegmentEvent>> intervalOrdering(final Interval selectedInterval) {
        return new Ordering<Map.Entry<Segment, SegmentEvent>>() {
            @Override
            public int compare(Map.Entry<Segment, SegmentEvent> left, Map.Entry<Segment, SegmentEvent> right) {
                Duration leftDuration = overlapDurationOf(selectedInterval, intervalFrom(left.getKey(), left.getValue()));
                Duration rightDuration = overlapDurationOf(selectedInterval, intervalFrom(right.getKey(), right.getValue()));
                return rightDuration.compareTo(leftDuration);
            }
        };
    }

    private Duration overlapDurationOf(Interval selectedInterval, Interval candidateInterval) {
        return selectedInterval.overlap(candidateInterval).toDuration();
    }

    private Interval intervalFrom(Segment key, SegmentEvent value) {
        return new Interval(value.getOffset().getStandardSeconds(), value.getOffset()
                .plus(key.getDuration()).getStandardSeconds());
    }

    private ImmutableMultimap<Segment, SegmentEvent> filterNonOverlapping(
            final Interval selectedInterval, Multimap<Segment, SegmentEvent> segmentMap) {

        ImmutableMultimap.Builder<Segment, SegmentEvent> filteredSegments = ImmutableMultimap.builder();
        for (Map.Entry<Segment, SegmentEvent> entry : segmentMap.entries()) {
            if (overlappingIntervalPredicate(selectedInterval).apply(entry)) {
                filteredSegments.put(entry.getKey(), entry.getValue());
            }
        }
        return filteredSegments.build();
    }


    private Predicate<Map.Entry<Segment, SegmentEvent>> overlappingIntervalPredicate(final Interval selectedInterval) {
        return new Predicate<Map.Entry<Segment, SegmentEvent>>() {
            @Override
            public boolean apply(final Map.Entry<Segment, SegmentEvent> input) {
                Duration duration = input.getKey().getDuration();
                Duration offset = input.getValue().getOffset();
                Interval candidateInterval = new Interval(
                        offset.getStandardSeconds(), offset.plus(duration).getStandardSeconds()
                );
                return candidateInterval.overlaps(selectedInterval);
            }
        };
    }
}
