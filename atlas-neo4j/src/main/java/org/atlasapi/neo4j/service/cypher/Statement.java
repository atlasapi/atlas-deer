package org.atlasapi.neo4j.service.cypher;

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;

public class Statement {

    private final ImmutableList<Clause> clauses;

    private Statement(Clause... clauses) {
        this.clauses = ImmutableList.copyOf(clauses);
    }

    public static Statement statement(Clause... clauses) {
        return new Statement(clauses);
    }

    public String build() {
        validate();

        return clauses.stream()
                .map(Clause::build)
                .collect(Collectors.joining(" "));
    }

    private void validate() {
        checkArgument(!clauses.isEmpty());

        // TODO more validation
    }
}
