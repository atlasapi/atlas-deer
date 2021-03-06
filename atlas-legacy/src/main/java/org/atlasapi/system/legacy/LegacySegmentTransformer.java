package org.atlasapi.system.legacy;

import org.atlasapi.entity.Alias;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentType;

public class LegacySegmentTransformer
        extends DescribedLegacyResourceTransformer<org.atlasapi.media.segment.Segment, Segment> {

    @Override
    protected Segment createDescribed(org.atlasapi.media.segment.Segment input) {
        Segment segment = new Segment();
        if (input.getType() != null) {
            segment.setType(SegmentType.valueOf(input.getType().name()));
        }
        segment.setDuration(input.getDuration());
        return segment;
    }

    @Override
    protected Iterable<Alias> moreAliases(org.atlasapi.media.segment.Segment input) {
        return super.transformAliases(input);
    }
}
