package org.atlasapi.query.v4.search.coercer;

public class Range<T> {

    private final T from;
    private final T to;

    public Range(T from, T to) {
        this.from = from;
        this.to = to;
    }

    public T getFrom() {
        return from;
    }

    public T getTo() {
        return to;
    }
}
