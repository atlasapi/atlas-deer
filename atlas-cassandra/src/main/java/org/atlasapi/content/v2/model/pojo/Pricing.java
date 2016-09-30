package org.atlasapi.content.v2.model.pojo;

import org.joda.time.Instant;

public class Pricing {

    private Instant start;
    private Instant end;
    private Price price;

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

    public Price getPrice() {
        return price;
    }

    public void setPrice(Price price) {
        this.price = price;
    }
}
