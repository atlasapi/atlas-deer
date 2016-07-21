package org.atlasapi.neo4j.service.model;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jPersistenceException extends RuntimeException {

    private Neo4jPersistenceException(String message) {
        super(
                checkNotNull(message)
        );
    }

    private Neo4jPersistenceException(String message, Throwable cause) {
        super(
                checkNotNull(message),
                checkNotNull(cause)
        );
    }

    public static Neo4jPersistenceException create(String message) {
        return new Neo4jPersistenceException(message);
    }

    public static Neo4jPersistenceException create(String message, Throwable cause) {
        return new Neo4jPersistenceException(message, cause);
    }
}
