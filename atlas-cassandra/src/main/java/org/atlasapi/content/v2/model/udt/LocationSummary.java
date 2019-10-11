package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "locationsummary")
public class LocationSummary {

    @Field(name = "available") private Boolean available;
    @Field(name = "uri") private String uri;
    @Field(name = "interval") @Frozen private Interval interval;

    public LocationSummary() {}

    public Boolean getAvailable() {
        return available;
    }

    public void setAvailable(Boolean available) {
        this.available = available;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public Interval getInterval() {
        return interval;
    }

    public void setInterval(Interval interval) {
        this.interval = interval;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        LocationSummary that = (LocationSummary) object;
        return Objects.equals(available, that.available) &&
                Objects.equals(uri, that.uri) &&
                Objects.equals(interval, that.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, uri, interval);
    }
}
