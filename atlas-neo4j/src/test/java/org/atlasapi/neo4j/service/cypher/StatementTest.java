package org.atlasapi.neo4j.service.cypher;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.atlasapi.neo4j.service.cypher.Delete.delete;
import static org.atlasapi.neo4j.service.cypher.Match.match;
import static org.atlasapi.neo4j.service.cypher.Node.node;
import static org.atlasapi.neo4j.service.cypher.Pattern.pattern;
import static org.atlasapi.neo4j.service.cypher.Statement.statement;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StatementTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void statementWithNoClausesFailsValidation() throws Exception {
        exception.expect(IllegalArgumentException.class);
        statement()
                .build();
    }

    @Test
    public void deleteStatement() throws Exception {
        String statement = statement(
                match(pattern(
                        node().name("a")
                )),
                delete("a")
        )
                .build();

        assertThat(statement, is("MATCH (a) DELETE a"));
    }
}
