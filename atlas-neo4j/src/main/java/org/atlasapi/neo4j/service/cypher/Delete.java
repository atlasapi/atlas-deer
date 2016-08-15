package org.atlasapi.neo4j.service.cypher;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;

public class Delete implements Clause {

    private final ImmutableList<String> variables;

    private Delete(String... variables) {
        this.variables = ImmutableList.copyOf(variables);

        checkArgument(!this.variables.isEmpty());
    }

    public static Delete delete(String... variables) {
        return new Delete(variables);
    }

    @Override
    public String build() {
        return "DELETE " + Joiner.on(", ").join(variables);
    }
}
