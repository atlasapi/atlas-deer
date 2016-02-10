package org.atlasapi.segment;

import org.atlasapi.content.Described;

import com.google.common.base.Function;
import org.joda.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

public class Segment extends Described {

    private SegmentType type;
    private Duration duration;

    public SegmentRef toRef() {
        return new SegmentRef(checkNotNull(
                this.getId(),
                "Can't create reference for segment without ID"
        ), publisher);
    }

    public SegmentType getType() {
        return this.type;
    }

    public void setType(SegmentType type) {
        this.type = type;
    }

    public Duration getDuration() {
        return this.duration;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    @Override
    public Described copy() {
        throw new UnsupportedOperationException();
    }

    public static final Function<Segment, SegmentRef> TO_REF = new Function<Segment, SegmentRef>() {

        @Override
        public SegmentRef apply(Segment input) {
            return input.toRef();
        }
    };

}
