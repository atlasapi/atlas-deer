package org.atlasapi.content;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public class Priority {

    private ImmutableList<String> reasons;
    private Double priority;

    public Priority(Double priority, Iterable<String> reasons) {
        this.reasons = ImmutableList.copyOf(reasons);
        this.priority = priority;
    }

    public Double getPriority() {
        return priority;
    }

    public ImmutableList<String> getReasons() {
        return reasons;
    }

    public void setPriority(Double priority) {
        this.priority = priority;
    }

    public void setReasons(ImmutableList<String> reasons) {
        this.reasons = reasons;
    }

    public void copyTo(Priority priority) {
        priority.setPriority(this.priority);
        priority.setReasons(this.reasons);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(reasons, priority);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Priority other = (Priority) obj;
        return Objects.equal(this.reasons, other.reasons)
                && Objects.equal(this.priority, other.priority);
    }
}
