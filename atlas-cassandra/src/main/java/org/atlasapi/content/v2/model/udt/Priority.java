package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.util.NullOrEmptyEquality;

import java.util.List;
import java.util.Objects;

@UDT(name = "priority")
public class Priority {

    @Field(name = "positive") private List<String> positive;
    @Field(name = "negative") private List<String> negative;
    @Field(name = "priority") private Double priority;

    public Priority() {}

    public List<String> getPositive() {
        return positive;
    }

    public void setPositive(List<String> positive) {
        this.positive = positive;
    }

    public List<String> getNegative() {
        return negative;
    }

    public void setNegative(List<String> negative) {
        this.negative = negative;
    }

    public Double getPriority() {
        return priority;
    }

    public void setPriority(Double priority) {
        this.priority = priority;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Priority priority1 = (Priority) object;
        return NullOrEmptyEquality.equals(positive, priority1.positive) &&
                NullOrEmptyEquality.equals(negative, priority1.negative) &&
                Objects.equals(priority, priority1.priority);
    }
}
