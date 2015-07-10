package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryOrdering {

    private final String path;
    private final boolean ascending;

    private QueryOrdering(String path, boolean ascending) {
        this.path = checkNotNull(path);
        this.ascending = ascending;
    }

    public static QueryOrdering fromOrderBy(String orderBy) {
        int lastDot = orderBy.lastIndexOf(".");
        if (lastDot == -1) {
            throw new IllegalArgumentException("Missing .asc or .desc operator after " + orderBy);
        }
        String path = orderBy.substring(0, lastDot);
        String order = orderBy.substring(lastDot + 1, orderBy.length());
        if (order.equalsIgnoreCase("asc")) {
            return new QueryOrdering(path, true);
        } else {
            return new QueryOrdering(path, false);
        }
    }

    public boolean isAscending() {
        return ascending;
    }

    public String getPath() {
        return path;
    }
}
