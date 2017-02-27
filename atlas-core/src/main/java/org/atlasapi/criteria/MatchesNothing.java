package org.atlasapi.criteria;

/**
 * I represent a query that is never satisfied by any content.  Intermediate query processors may
 * replace satisfiable queries (or subqueries) with me to indicate that they have determined that no
 * content matches a query.
 *
 * @author John Ayres (john@metabroadcast.com)
 */
public class MatchesNothing extends AtomicQuery {

    private static MatchesNothing INSTANCE = new MatchesNothing();

    public static MatchesNothing get() {
        return INSTANCE;
    }

    private MatchesNothing() { /* SINGLETON */ }

    public <V> V accept(QueryVisitor<V> visitor) {
        return visitor.visit(this);
    }
}
