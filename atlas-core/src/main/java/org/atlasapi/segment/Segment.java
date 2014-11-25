package org.atlasapi.segment;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.Described;
import org.atlasapi.content.Description;
import org.atlasapi.content.Identified;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Duration;

import com.google.common.base.Function;

public class Segment extends Described {

    private SegmentType type;
    private Duration duration;
    private Publisher publisher;

    public SegmentRef toRef() {
        return new SegmentRef(checkNotNull(this.getId().longValue(), "Can't create reference for segment without ID"));
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

    public Publisher getPublisher() {
        return this.publisher;
    }

    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public Described copy() {
        return null;
    }

    public static final Function<Segment, SegmentRef> TO_REF = new Function<Segment, SegmentRef>() {
        @Override
        public SegmentRef apply(Segment input) {
            return input.toRef();
        }
    };
    
}
