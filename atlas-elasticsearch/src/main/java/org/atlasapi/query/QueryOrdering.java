package org.atlasapi.query;

import java.util.Arrays;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryOrdering {

    private final ImmutableList<Clause> sortOrder;

    private QueryOrdering(ImmutableList<Clause> sortOrder) {
        this.sortOrder = checkNotNull(sortOrder);
    }

    public static QueryOrdering fromOrderBy(String orderBy) {
        return new QueryOrdering(Arrays.stream(orderBy.split(","))
                .map(clause -> {
                    int lastDot = clause.lastIndexOf(".");
                    if (lastDot == -1) {
                        return new Clause(clause, Direction.ASC);
                    }

                    String path = clause.substring(0, lastDot);
                    Direction direction = Direction.valueOf(clause.substring(
                            lastDot + 1,
                            clause.length()
                    ).toUpperCase());

                    return new Clause(path, direction);
                })
                .collect(MoreCollectors.toImmutableList()));
    }

    public ImmutableList<Clause> getSortOrder() {
        return sortOrder;
    }

    public enum Direction {
        ASC,
        DESC
    }

    public static class Clause {

        private final String path;
        private final Direction direction;

        public Clause(String path, Direction direction) {
            this.path = checkNotNull(path);
            this.direction = checkNotNull(direction);
        }

        public boolean isAscending() {
            return direction == Direction.ASC;
        }

        public String getPath() {
            return path;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path)
                    .add("direction", direction)
                    .toString();
        }
    }
}
