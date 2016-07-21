package org.atlasapi.neo4j.service.writers;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.neo4j.service.model.Neo4jPersistenceException;

import com.google.common.collect.ImmutableMap;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementRunner;

public class ContentWriter {

    public static final String CONTENT_ID = "id";
    public static final String CONTENT_SOURCE = "source";

    private final Statement writeContentRefStatement;

    private ContentWriter() {
        writeContentRefStatement = new Statement(""
                + "MERGE (content:Content { " + CONTENT_ID + ": " + parameter(CONTENT_ID) + " }) "
                + "SET content." + CONTENT_SOURCE + " = " + parameter(CONTENT_SOURCE) + " "
                + "RETURN id(content) AS id"
        );
    }

    public static ContentWriter create() {
        return new ContentWriter();
    }

    public Long writeResourceRef(ResourceRef resourceRef, StatementRunner runner) {
        ImmutableMap<String, Object> statementParameters = ImmutableMap.of(
                CONTENT_ID, resourceRef.getId().longValue(),
                CONTENT_SOURCE, resourceRef.getSource().key()
        );

        return execute(
                writeContentRefStatement.withParameters(statementParameters),
                runner
        );
    }

    private Long execute(Statement statement, StatementRunner runner) {
        StatementResult result = runner.run(statement);

        if (result.hasNext()) {
            return result.next().get("id").asLong();
        }

        throw Neo4jPersistenceException.create(
                String.format("Failed to execute query <%s>", statement.text())
        );
    }

    private String parameter(String parameterName) {
        return "{" + parameterName + "}";
    }
}
