package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "locationsummary")
public class LocationSummary {

    private Boolean available;
    private String uri;
    private Instant start;
    private Instant end;

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

    public Instant getStart() {
        return start;
    }

    public void setStart(Instant start) {
        this.start = start;
    }

    public Instant getEnd() {
        return end;
    }

    public void setEnd(Instant end) {
        this.end = end;
    }
}
