package org.atlasapi.neo4j.service.query;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jQuery {

    private final String query;
    private final ImmutableMap<String, Object> parameters;

    private Neo4jQuery(String query, Map<String, Object> parameters) {
        this.query = checkNotNull(query);
        this.parameters = ImmutableMap.copyOf(parameters);
    }

    public static Neo4jQuery create(String query, Map<String, Object> parameters) {
        return new Neo4jQuery(query, parameters);
    }

    public String getQuery() {
        return query;
    }

    public ImmutableMap<String, Object> getParameters() {
        return parameters;
    }
}
