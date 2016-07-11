package org.atlasapi.content;

import java.util.List;

import com.google.common.base.Objects;

public class PriorityScoreReasons {

    private List<String> positive;
    private List<String> negative;

    public PriorityScoreReasons() {

    }

    public PriorityScoreReasons(List<String> positive, List<String> negative) {
        this.positive = positive;
        this.negative = negative;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PriorityScoreReasons that = (PriorityScoreReasons) o;

        if (positive != null ? !positive.equals(that.positive) : that.positive != null) {
            return false;
        }
        return !(negative != null ? !negative.equals(that.negative) : that.negative != null);

    }

    @Override
    public int hashCode() {
        return Objects.hashCode(positive, negative);
    }
}
