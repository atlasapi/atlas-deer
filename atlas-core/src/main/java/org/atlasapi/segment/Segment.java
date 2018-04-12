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

    public static Segment copyTo(Segment from, Segment to) {
        Described.copyTo(from, to);
        to.type = from.type;
        to.duration = from.duration;
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Segment) {
            copyTo(this, (Segment) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public Segment copy() {
        return copyTo(this, new Segment());
    }

    public static final Function<Segment, SegmentRef> TO_REF = Segment::toRef;

}
