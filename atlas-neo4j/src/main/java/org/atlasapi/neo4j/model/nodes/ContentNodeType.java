package org.atlasapi.neo4j.model.nodes;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.atlasapi.util.ImmutableCollectors;

public enum ContentNodeType {
    BRAND("Brand"),
    SERIES("Series"),
    EPISODE("Episode"),
    ITEM("Item"),
    FILM("Film"),
    SONG("Song"),
    CLIP("Clip");

    private final String type;

    private static final Map<String, ContentNodeType> typeIndex =
            Arrays.stream(ContentNodeType.values())
                    .collect(ImmutableCollectors.toMap(
                            contentNodeType -> contentNodeType.getType().toLowerCase(),
                            Function.identity()
                    ));

    ContentNodeType(String type) {
        this.type = type;
    }

    public static Optional<ContentNodeType> from(String type) {
        return Optional.ofNullable(typeIndex.get(type.toLowerCase()));
    }

    public String getType() {
        return type;
    }
}
