package org.atlasapi.neo4j.service.cypher;

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;

public class Match implements Clause {

    private final boolean optional;
    private final ImmutableList<Pattern> patterns;

    private Match(boolean optional, Pattern... patterns) {
        this.optional = optional;
        this.patterns = ImmutableList.copyOf(patterns);

        checkArgument(!this.patterns.isEmpty());
    }

    public static Match match(Pattern... patterns) {
        return new Match(false, patterns);
    }

    public static Match matchOptional(Pattern... patterns) {
        return new Match(true, patterns);
    }

    @Override
    public String build() {
        StringBuilder builder = new StringBuilder();

        if (optional) {
            builder.append("OPTIONAL MATCH ");
        } else {
            builder.append("MATCH ");
        }

        builder.append(
            patterns.stream()
                    .map(Pattern::build)
                    .collect(Collectors.joining(", "))
        );

        return builder.toString();
    }
}
