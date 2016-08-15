package org.atlasapi.neo4j.service.cypher;

import java.util.Optional;

import com.google.common.base.Joiner;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Edge extends GraphObject<Edge> {

    private final EdgeDirection direction;
    private final Optional<Length> length;

    private Edge(EdgeDirection direction, Optional<Length> length) {
        this.direction = checkNotNull(direction);
        this.length = checkNotNull(length);
    }

    public static Edge edge(EdgeDirection direction) {
        return new Edge(direction, Optional.empty());
    }

    public static Edge boundedPatternEdge(EdgeDirection direction, int from, int to) {
        return new Edge(direction, Optional.of(Length.bounded(from, to)));
    }

    public static Edge leftBoundedPatternEdge(EdgeDirection direction, int from) {
        return new Edge(direction, Optional.of(Length.leftBounded(from)));
    }

    public static Edge rightBoundedPatternEdge(EdgeDirection direction, int to) {
        return new Edge(direction, Optional.of(Length.rightBounded(to)));
    }

    public static Edge fixedPatternEdge(EdgeDirection direction, int length) {
        return new Edge(direction, Optional.of(Length.fixed(length)));
    }

    public static Edge unboundedPatternEdge(EdgeDirection direction) {
        return new Edge(direction, Optional.of(Length.unbounded()));
    }

    @Override
    public String build() {
        StringBuilder builder = new StringBuilder();

        if (direction == EdgeDirection.LEFT) {
            builder.append("<-");
        } else {
            builder.append("-");
        }

        if (hasPatternData()) {
            builder.append("[");
        }

        if (name.isPresent()) {
            builder.append(name.get());
        }

        if (label.isPresent()) {
            builder.append(":");
            builder.append(label.get());
        }

        if (!properties.isEmpty()) {
            builder.append(" {");

            builder.append(
                    Joiner.on(", ")
                            .withKeyValueSeparator(": ")
                            .join(properties)
            );

            builder.append("}");
        }

        if (length.isPresent()) {
            builder.append(length.get().build());
        }

        if (hasPatternData()) {
            builder.append("]");
        }

        if (direction == EdgeDirection.RIGHT) {
            builder.append("->");
        } else {
            builder.append("-");
        }

        return builder.toString();
    }

    @Override
    protected Edge getThis() {
        return this;
    }

    private boolean hasPatternData() {
        return name.isPresent() || label.isPresent() || !properties.isEmpty() || length.isPresent();
    }

    private static class Length {

        private final Optional<Integer> from;
        private final Optional<Integer> to;

        private Length(Optional<Integer> from, Optional<Integer> to) {
            this.from = checkNotNull(from);
            this.to = checkNotNull(to);

            if (from.isPresent() && to.isPresent()) {
                checkArgument(from.get() <= to.get());
            }

            from.ifPresent(value -> checkArgument(value >= 0));
            to.ifPresent(value -> checkArgument(value >= 0));
        }

        public static Length bounded(int from, int to) {
            return new Length(Optional.of(from), Optional.of(to));
        }

        public static Length leftBounded(int from) {
            return new Length(Optional.of(from), Optional.empty());
        }

        public static Length rightBounded(int to) {
            return new Length(Optional.empty(), Optional.of(to));
        }

        public static Length fixed(int length) {
            return new Length(Optional.of(length), Optional.of(length));
        }

        public static Length unbounded() {
            return new Length(Optional.empty(), Optional.empty());
        }

        public String build() {
            StringBuilder builder = new StringBuilder();

            builder.append("*");

            if (from.isPresent() && to.isPresent()) {
                if (from.get().equals(to.get())) {
                    // Fixed length
                    builder.append(from.get());
                } else {
                    // Fully bounded length
                    builder.append(from.get())
                            .append("..")
                            .append(to.get());
                }
            }
            else if (from.isPresent()) {
                // Left bounded length
                builder.append(from.get())
                        .append("..");
            } else if (to.isPresent()) {
                // Right bounded length
                builder.append("..")
                        .append(to.get());
            }

            return builder.toString();
        }
    }
}
