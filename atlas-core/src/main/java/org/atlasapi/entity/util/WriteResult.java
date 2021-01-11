package org.atlasapi.entity.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ComparisonChain;
import com.metabroadcast.common.time.DateTimeZones;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents the result of a call to a write/store procedure, for use with *Store types.
 * <p>
 * Immutable given W is immutable.
 *
 * @param <RESOURCE> - type of the written resource
 */
public final class WriteResult<RESOURCE, PREVIOUS> implements Comparable<WriteResult<?, ?>> {

    public static final Predicate<WriteResult<?, ?>> WRITTEN
            = new Predicate<WriteResult<?, ?>>() {

        @Override
        public boolean apply(@Nullable WriteResult<?, ?> input) {
            return input.written();
        }
    };

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static final <R, P> Predicate<WriteResult<? extends R, ? extends P>> writtenFilter() {
        return (Predicate) WRITTEN;
    }

    public static final <R, P> Builder<R, P> result(R resource, boolean wasWritten) {
        return new Builder<R, P>(resource, wasWritten, new DateTime(DateTimeZones.UTC));
    }

    public static final <R, P> Builder<R, P> unwritten(R resource) {
        return result(resource, false);
    }

    public static final <R, P> Builder<R, P> written(R resource) {
        return result(resource, true);
    }

    public static final class Builder<R, P> {

        private R resource;
        private boolean written;
        private DateTime writeTime;
        private P previous;

        public Builder(R resource, boolean written, DateTime writeTime) {
            this.resource = resource;
            this.written = written;
            this.writeTime = writeTime;
        }

        public Builder<R, P> withPrevious(P previous) {
            this.previous = previous;
            return this;
        }

        public WriteResult<R, P> build() {
            return new WriteResult<R, P>(resource, written, writeTime, previous);
        }
    }

    private final RESOURCE resource;
    private final boolean written;
    private final DateTime writeTime;
    private final Optional<PREVIOUS> previous;

    public WriteResult(RESOURCE resource, boolean written, DateTime writeTime,
            @Nullable PREVIOUS previous) {
        this.resource = checkNotNull(resource);
        this.written = written;
        this.writeTime = checkNotNull(writeTime);
        this.previous = Optional.fromNullable(previous);
    }

    /**
     * The resource as it was written when the write was attempted.
     *
     * @return - the resource
     */
    public RESOURCE getResource() {
        return resource;
    }

    /**
     * Indicates whether the resource was actually written at this call to write.
     *
     * @return true iff the resource was written.
     */
    public boolean written() {
        return written;
    }

    /**
     * The time at which the write was attempted.
     *
     * @return
     */
    public DateTime getWriteTime() {
        return writeTime;
    }

    /**
     * The previous version of the resource.
     *
     * @return
     */
    public Optional<PREVIOUS> getPrevious() {
        return previous;
    }

    @Override
    public int compareTo(WriteResult<?, ?> other) {
        return ComparisonChain.start()
                .compare(writeTime, other.writeTime)
                .compareFalseFirst(written, other.written)
                .compare(resource.hashCode(), other.resource.hashCode())
                .result();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof WriteResult<?, ?>) {
            WriteResult<?, ?> other = (WriteResult<?, ?>) that;
            return written == other.written
                    && writeTime.equals(other.writeTime)
                    && resource.equals(other.resource);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(resource, writeTime, written);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("resource", resource.toString())
                .add("written", written)
                .add("at", writeTime)
                .toString();
    }

}
