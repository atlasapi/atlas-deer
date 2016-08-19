package org.atlasapi.neo4j.service.resolvers;

import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

public abstract class Neo4jResolver {

    protected List<Record> read(Statement statement, StatementRunner runner) {
        return runner.run(statement).list();
    }

    protected String parameter(String parameterName) {
        return "{" + parameterName + "}";
    }
}
