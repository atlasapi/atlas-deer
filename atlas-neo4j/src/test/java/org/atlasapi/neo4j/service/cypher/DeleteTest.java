package org.atlasapi.neo4j.service.cypher;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DeleteTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void deleteWithNoVariablesFailsValidation() throws Exception {
        exception.expect(IllegalArgumentException.class);
        Delete.delete()
                .build();
    }

    @Test
    public void deleteWithSingleVariable() throws Exception {
        String delete = Delete.delete("a")
                .build();

        assertThat(delete, is("DELETE a"));
    }

    @Test
    public void deleteWithMultipleVariables() throws Exception {
        String delete = Delete.delete("a", "b")
                .build();

        assertThat(delete, is("DELETE a, b"));
    }
}
