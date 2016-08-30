package org.atlasapi.neo4j.service.writers;

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

public abstract class Neo4jWriter {

    protected void write(Statement statement, StatementRunner runner) {
        // We don't need to consume the statement result because the driver will do that when
        // the statement runner is closed/committed. Not consuming the result here allows the
        // driver to do some additional optimisations by lazily resolving statements only when
        // it needs to.
        runner.run(statement);
    }

    protected String param(String parameterName) {
        return "{" + parameterName + "}";
    }
}
