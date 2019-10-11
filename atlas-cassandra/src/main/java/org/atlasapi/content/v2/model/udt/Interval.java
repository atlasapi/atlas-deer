package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.util.Objects;

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

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Interval interval = (Interval) object;
        return Objects.equals(start, interval.start) &&
                Objects.equals(end, interval.end);
    }
}
