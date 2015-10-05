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
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;

/**
 *  This is a client-specific strategy for merging together and ordering the set of {@link RelatedLink}
 *  from a set of {@link org.atlasapi.segment.Segment} and {@link org.atlasapi.segment.SegmentEvent}.
 */
public class ScrubbablesSegmentRelatedLinkMerger implements SegmentRelatedLinkMerger {

    public static final Function<Map.Entry<Segment, SegmentEvent>, Set<RelatedLink>> PROJECT_RELATED_LINKS =
            new Function<Map.Entry<Segment, SegmentEvent>, Set<RelatedLink>>() {
        @Nullable
        @Override
        public Set<RelatedLink> apply(Map.Entry<Segment, SegmentEvent> input) {
            return input.getKey().getRelatedLinks();
        }
    };

    /**
     * Given a Segment, SegmentEvent and a mapping from each, this method will merge and sort the
     * RelatedLinks found on each Segment. For each Segment & Event pair in the map, the interval defined
     * by their offset and duration is compared to the 'selected' pair's. Intervals that overlap
     * the most with the selected Segment & Event are ordered higher. Pairs that do not overlap do not
     * have their RelatedLinks merged in.
     *
     * @param selectedSegment - The Segment that defines the duration of interval.
     * @param selectedSegmentEvent - The SegmentEvent that defined the offset of the interval.
     * @param segmentMap - Other Segment & Event pairs to merge RelatedLinks in from.
     *
     * @return An ordered list of all RelatedLinks and overlap
     */
    @Override
    public ImmutableList<RelatedLink> getLinks(Segment selectedSegment, SegmentEvent selectedSegmentEvent,
                                               Multimap<Segment, SegmentEvent> segmentMap) {

        Interval selectedInterval = intervalFrom(selectedSegment, selectedSegmentEvent);

        return FluentIterable.from(
                greatestOverlapDurationOrdering(selectedInterval).immutableSortedCopy(
                        filterNonOverlapping(selectedInterval, segmentMap).entries()
                )
        ).transformAndConcat(PROJECT_RELATED_LINKS).toList();
    }

    private Ordering<Map.Entry<Segment, SegmentEvent>> greatestOverlapDurationOrdering(final Interval selectedInterval) {
        return new Ordering<Map.Entry<Segment, SegmentEvent>>() {
            @Override
            public int compare(Map.Entry<Segment, SegmentEvent> left, Map.Entry<Segment, SegmentEvent> right) {
                return overlapDurationOf(selectedInterval, intervalFrom(right.getKey(), right.getValue()))
                        .compareTo(
                        overlapDurationOf(selectedInterval, intervalFrom(left.getKey(), left.getValue())));
            }
        };
    }

    /*
     * Returns the overlap Duration of two Intervals.
     */
    private Duration overlapDurationOf(Interval selectedInterval, Interval candidateInterval) {
        return selectedInterval.overlap(candidateInterval).toDuration();
    }

    /*
     * Creates the interval defined by the offset and duration found on a Segment & SegmentEvent.
     */
    private Interval intervalFrom(Segment key, SegmentEvent value) {
        return new Interval(
                value.getOffset().getStandardSeconds(),
                value.getOffset().plus(key.getDuration()).getStandardSeconds()
        );
    }

    private ImmutableMultimap<Segment, SegmentEvent> filterNonOverlapping(
            final Interval selectedInterval, Multimap<Segment, SegmentEvent> segmentMap) {

        return ImmutableMultimap.copyOf(
                Multimaps.filterEntries(segmentMap, overlappingIntervalPredicate(selectedInterval))
        );
    }

    /*
     * Returns a predicate for asserting whether or not the interval defined by a Segment & SegmentEvent pair
     * overlaps the passed interval.
     */
    private Predicate<Map.Entry<Segment, SegmentEvent>> overlappingIntervalPredicate(final Interval selectedInterval) {
        return new Predicate<Map.Entry<Segment, SegmentEvent>>() {

            @Override
            public boolean apply(final Map.Entry<Segment, SegmentEvent> input) {
                return intervalFrom(input.getKey(), input.getValue()).overlaps(selectedInterval);
            }
        };
    }
}