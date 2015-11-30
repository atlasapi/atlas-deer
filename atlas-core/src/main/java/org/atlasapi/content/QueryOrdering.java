package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;

import org.atlasapi.util.ImmutableCollectors;

import com.google.common.collect.ImmutableList;

public class QueryOrdering {

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
    }

    private final ImmutableList<Clause> sortOrder;

    public QueryOrdering(ImmutableList<Clause> sortOrder) {
        this.sortOrder = checkNotNull(sortOrder);
    }

    public ImmutableList<Clause> getSortOrder() {
        return sortOrder;
    }

    public static QueryOrdering fromOrderBy(String orderBy) {
        return new QueryOrdering(Arrays.stream(orderBy.split(","))
                .map(clause -> {
                    int lastDot = clause.lastIndexOf(".");
                    if (lastDot == -1) {
                        throw new IllegalArgumentException("Missing .asc or .desc operator after " + clause);
                    }

                   String path = clause.substring(0, lastDot);
                   Direction direction = Direction.valueOf(clause.substring(lastDot + 1, clause.length()).toUpperCase());

                    return new Clause(path, direction);
                })
                .collect(ImmutableCollectors.toList()));

    }
}
