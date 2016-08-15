package org.atlasapi.neo4j.service.cypher;

import com.google.common.base.Joiner;

public class Node extends GraphObject<Node> {

    private Node() {
        super();
    }

    public static Node node() {
        return new Node();
    }

    @Override
    public String build() {
        StringBuilder builder = new StringBuilder();

        builder.append("(");

        if (name.isPresent()) {
            builder.append(name.get());
        }

        if (label.isPresent()) {
            builder.append(":");
            builder.append(label.get());
        }

        if (!properties.isEmpty()) {
            builder.append(" {");

            builder.append(
                    Joiner.on(", ")
                            .withKeyValueSeparator(": ")
                            .join(properties)
            );

            builder.append("}");
        }

        builder.append(")");

        return builder.toString();
    }

    @Override
    protected Node getThis() {
        return this;
    }
}
