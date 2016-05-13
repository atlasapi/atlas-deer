package org.atlasapi.application;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Objects;

public class SourceReadEntry {

    private Publisher publisher;
    private SourceStatus sourceStatus;

    public SourceReadEntry(
            @Nullable Publisher publisher,
            @Nullable SourceStatus sourceStatus
    ) {
        this.publisher = publisher;
        this.sourceStatus = sourceStatus;
    }

    @Nullable
    public Publisher getPublisher() {
        return publisher;
    }

    @Nullable
    public SourceStatus getSourceStatus() {
        return sourceStatus;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SourceReadEntry) {
            SourceReadEntry other = (SourceReadEntry) obj;
            return Objects.equal(this.getPublisher(), other.getPublisher())
                    && Objects.equal(this.getSourceStatus(), other.getSourceStatus());
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("Publisher", this.getPublisher())
                .add("Status", this.getSourceStatus())
                .toString();
    }

}
