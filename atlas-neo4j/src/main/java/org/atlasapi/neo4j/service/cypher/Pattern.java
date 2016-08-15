package org.atlasapi.neo4j.service.cypher;

import java.util.List;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkArgument;

public class Pattern {

    private final List<GraphObject> pattern;

    private Pattern(GraphObject... graphObjects) {
        this.pattern = ImmutableList.copyOf(graphObjects);
        validate();
    }

    public static Pattern pattern(GraphObject... graphObjects) {
        return new Pattern(graphObjects);
    }

    public String build() {
        StringBuilder builder = new StringBuilder();

        for (GraphObject graphObject : pattern) {
            builder.append(graphObject.build());
        }

        return builder.toString();
    }

    private void validate() {
        checkArgument(!pattern.isEmpty());

        boolean lastObjectIsEdge = true;
        for (GraphObject graphObject : pattern) {
            if (lastObjectIsEdge) {
                checkArgument(graphObject instanceof Node);
                lastObjectIsEdge = false;
            } else {
                checkArgument(graphObject instanceof Edge);
                lastObjectIsEdge = true;
            }
        }
    }
}
