package org.atlasapi.content;

import org.atlasapi.hashing.Hashable;

import com.metabroadcast.common.currency.Price;

import com.google.common.base.Objects;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;

public class Pricing implements Hashable {

    private final DateTime startTime;
    private final DateTime endTime;
    private final Price price;

    public Pricing(DateTime startTime, DateTime endTime, Price price) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.price = checkNotNull(price);
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public DateTime getEndTime() {
        return endTime;
    }

    public Price getPrice() {
        return price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pricing pricing = (Pricing) o;
        return Objects.equal(startTime, pricing.startTime) &&
                Objects.equal(endTime, pricing.endTime) &&
                Objects.equal(price, pricing.price);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(startTime, endTime, price);
    }
}
