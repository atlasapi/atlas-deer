package org.atlasapi.neo4j.service.cypher;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Statement {

    private final List<Clause> clauses;

    private Statement() {
        this.clauses = Lists.newArrayList();
    }

    public static Statement statement() {
        return new Statement();
    }

    public Statement clause(Clause clause) {
        this.clauses.add(checkNotNull(clause));
        return this;
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
