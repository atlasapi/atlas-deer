package org.atlasapi.neo4j.service.writers;

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

public abstract class Neo4jWriter {

    protected void write(Statement statement, StatementRunner runner) {
        runner.run(statement).consume();
    }

    protected String parameter(String parameterName) {
        return "{" + parameterName + "}";
    }
}
