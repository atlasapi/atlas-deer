package org.atlasapi.content;

import java.util.Arrays;

import org.atlasapi.util.ImmutableCollectors;

import com.google.common.collect.ImmutableList;

public class QueryOrdering {

    public enum Direction {
        ASC,
        DESC
    }

    public static class Node {
        private final String path;
        private final Direction direction;

        public Node(String path, Direction direction) {
            this.path = path;
            this.direction = direction;
        }

        public boolean isAscending() {
            return direction == Direction.ASC;
        }

        public String getPath() {
            return path;
        }
    }

    private final ImmutableList<Node> sortOrder;

    public QueryOrdering(ImmutableList<Node> sortOrder) {
        this.sortOrder = sortOrder;
    }

    public ImmutableList<Node> getSortOrder() {
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

                    return new Node(path, direction);
                })
                .collect(ImmutableCollectors.toList()));

    }
}
