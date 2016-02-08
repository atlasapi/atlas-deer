package org.atlasapi.system.legacy;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.segment.SegmentRef;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentStore;

import com.metabroadcast.common.base.Maybe;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacySegmentMigrator {

    public static final Function<Id, SegmentRef> ID_TO_SEGMENT_REF = new Function<Id, SegmentRef>() {

        @Override
        public SegmentRef apply(Id id) {
            return new SegmentRef(id.longValue());
        }
    };
    private final SegmentResolver legacySegmentResolver;
    private final SegmentStore segmentStore;
    private final LegacySegmentTransformer legacySegmentTransformer = new LegacySegmentTransformer();
    private final Logger log = LoggerFactory.getLogger(getClass());

    public LegacySegmentMigrator(org.atlasapi.media.segment.SegmentResolver legacySegmentResolver,
            SegmentStore segmentStore) {
        this.legacySegmentResolver = checkNotNull(legacySegmentResolver);
        this.segmentStore = checkNotNull(segmentStore);
    }

    public WriteResult<Segment, Segment> migrateLegacySegment(Id legacyId)
            throws UnresolvedLegacySegmentException {

        log.trace("Attempting to resolve legacy segment with ID: {} in Deer first", legacyId);
        Iterable<Segment> segments = segmentStore.resolveSegments(ImmutableList.of(legacyId));
        Optional<Segment> existingSegment = Optional.fromNullable(Iterables.getOnlyElement(
                segments,
                null
        ));

        if (existingSegment.isPresent()) {
            log.trace("Legacy segment already present in Deer, returning...");
            return WriteResult.<org.atlasapi.segment.Segment, org.atlasapi.segment.Segment>unwritten(
                    existingSegment.get())
                    .withPrevious(existingSegment.get())
                    .build();
        }

        org.atlasapi.media.segment.Segment owlSegment = resolveLegacySegment(legacyId);
        log.trace("Resolved legacy segment in Owl");
        Segment deerSegment = legacySegmentTransformer.apply(owlSegment);
        return segmentStore.writeSegment(deerSegment);
    }

    private org.atlasapi.media.segment.Segment resolveLegacySegment(Id legacyId)
            throws UnresolvedLegacySegmentException {
        checkNotNull(legacyId, "Cannot resolve null legacy segment ID");

        log.trace("Resolving legacy segment in Owl with ID: " + legacyId.longValue());

        Maybe<org.atlasapi.media.segment.Segment> maybeSegment = Iterables.getOnlyElement(
                legacySegmentResolver.resolveById(ImmutableList.of(ID_TO_SEGMENT_REF.apply(legacyId)))
                        .entrySet()
        ).getValue();

        if (maybeSegment == null || maybeSegment.isNothing()) {
            throw new UnresolvedLegacySegmentException("Unable to resolve Segment using ID: "
                    + legacyId.longValue());
        }

        return maybeSegment.requireValue();
    }
}