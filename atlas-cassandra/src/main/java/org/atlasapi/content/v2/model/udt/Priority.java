package org.atlasapi.content.v2.model.udt;

import java.util.List;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "priority")
public class Priority {

    @Field(name = "p")
    private List<String> positive;

    @Field(name = "n")
    private List<String> negative;

    @Field(name = "pr")
    private Double priority;

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
}
