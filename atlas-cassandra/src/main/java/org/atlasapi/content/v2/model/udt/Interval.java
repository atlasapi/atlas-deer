package org.atlasapi.content.v2.model.udt;

import javax.annotation.Nullable;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "interval")
public class Interval {

    @Field(name = "start") private Instant start;
    @Field(name = "end") private Instant end;

    public Interval() {}

    @Nullable
    public Instant getStart() {
        return start;
    }

    public void setStart(@Nullable Instant start) {
        this.start = start;
    }

    @Nullable
    public Instant getEnd() {
        return end;
    }

    public void setEnd(@Nullable Instant end) {
        this.end = end;
    }
}
