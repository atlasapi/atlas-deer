package org.atlasapi.content.v2.model.pojo;

import org.joda.time.Instant;

import java.util.Objects;

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

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Pricing pricing = (Pricing) object;
        return Objects.equals(start, pricing.start) &&
                Objects.equals(end, pricing.end) &&
                Objects.equals(price, pricing.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, price);
    }
}
