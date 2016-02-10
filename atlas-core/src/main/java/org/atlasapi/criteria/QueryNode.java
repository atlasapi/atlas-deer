package org.atlasapi.criteria;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

public abstract class QueryNode {

    private static final Ordering<QueryNode> PATH_ORDERING = Ordering.from(new Comparator<QueryNode>() {

        @Override
        public int compare(QueryNode qn1, QueryNode qn2) {
            return Ordering.<String>natural().lexicographical()
                    .compare(qn1.pathSegments, qn2.pathSegments);
        }
    });

    public static final Ordering<QueryNode> pathOrdering() {
        return PATH_ORDERING;
    }

    private List<String> pathSegments = ImmutableList.of();

    public QueryNode(List<String> pathSegments) {
        this.pathSegments = pathSegments;
    }

    public List<String> pathSegments() {
        return pathSegments;
    }

    public void setPathSegments(List<String> pathSegments) {
        this.pathSegments = pathSegments;
    }

    public abstract QueryNode add(AttributeQuery<?> query, List<String> path, int pathPos);

    public abstract StringBuilder prettyPrint(int depth);

    public abstract <V> V accept(QueryNodeVisitor<V> visitor);

    protected int commonPathLen(List<String> path, int pathPos) {
        int len = 0;
        List<String> segments = pathSegments;
        while (len < segments.size() && equal(segments.get(len), path.get(pathPos + len))) {
            len++;
        }
        return len;
    }

    private boolean equal(String left, String right) {
        return left.equals(right);
    }

    protected <T> List<T> drop(List<T> list, int start) {
        return list.subList(start, list.size());
    }

    protected <T> List<T> take(List<T> list, int end) {
        return list.subList(0, end);
    }

    public static final class TerminalNode extends QueryNode {

        private final AttributeQuery<?> query;

        public TerminalNode(List<String> pathSegments, AttributeQuery<?> query) {
            super(pathSegments);
            this.query = query;
        }

        public AttributeQuery<?> getQuery() {
            return query;
        }

        @Override
        public String toString() {
            return prettyPrint(0).toString();
        }

        @Override
        public QueryNode add(AttributeQuery<?> query, List<String> path, int pathPos) {
            int commonLen = commonPathLen(path, pathPos);
            int commonEnd = pathPos + commonLen;
            return new IntermediateNode(
                    take(pathSegments(), commonLen),
                    this.removeCommonPrefix(commonLen),
                    new TerminalNode(drop(path, commonEnd), query)
            );
        }

        private QueryNode removeCommonPrefix(int commonLen) {
            this.setPathSegments(drop(pathSegments(), commonLen));
            return this;
        }

        @Override
        public StringBuilder prettyPrint(int depth) {
            return Joiner.on("").appendTo(new StringBuilder(), Collections.nCopies(depth, "\t"))
                    .append(pathSegments())
                    .append("==>")
                    .append(getQuery());
        }

        @Override
        public <V> V accept(QueryNodeVisitor<V> visitor) {
            return visitor.visit(this);
        }

    }

    public static final class IntermediateNode extends QueryNode {

        private final Map<String, QueryNode> descendants;

        public IntermediateNode(List<String> pathSegments) {
            this(pathSegments, Maps.<String, QueryNode>newHashMap());
        }

        public IntermediateNode(List<String> pathSegments, QueryNode left, QueryNode right) {
            super(pathSegments);
            this.descendants = Maps.newHashMap();
            descendants.put(left.pathSegments().get(0), left);
            descendants.put(right.pathSegments().get(0), right);
        }

        public IntermediateNode(List<String> pathSegments, Map<String, QueryNode> descendants) {
            super(pathSegments);
            this.descendants = descendants;
        }

        public Collection<QueryNode> getDescendants() {
            return descendants.values();
        }

        public QueryNode add(AttributeQuery<?> query, List<String> path, int pathPos) {
            int commonLen = commonPathLen(path, pathPos);
            int commonEnd = pathPos + commonLen;
            if (commonLen == pathSegments().size()) {
                String key = path.get(commonEnd);
                QueryNode commonChild = descendants.get(key);
                descendants.put(key, commonChild != null ? commonChild.add(query, path, commonEnd)
                                                         : new TerminalNode(
                                                                 drop(path, commonEnd),
                                                                 query
                                                         ));
                return this;
            } else {
                return new IntermediateNode(
                        take(pathSegments(), commonLen),
                        new IntermediateNode(drop(pathSegments(), commonLen), this.descendants),
                        new TerminalNode(drop(path, commonEnd), query)
                );
            }
        }

        @Override
        public StringBuilder prettyPrint(int depth) {
            StringBuilder sb = Joiner.on("")
                    .appendTo(new StringBuilder(), Collections.nCopies(depth, "\t"))
                    .append(pathSegments())
                    .append("==>");
            for (QueryNode descendant : descendants.values()) {
                sb.append("\n").append(descendant.prettyPrint(depth + 1));
            }
            return sb;
        }

        @Override
        public <V> V accept(QueryNodeVisitor<V> visitor) {
            return visitor.visit(this);
        }

        @Override
        public String toString() {
            return prettyPrint(0).toString();
        }

    }

}