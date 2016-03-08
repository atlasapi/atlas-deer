package org.atlasapi.neo4j.model.nodes;

import java.util.Objects;

public abstract class EntityNode {

    private Long id;

    public Long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EntityNode entityNode = (EntityNode) o;
        return Objects.equals(id, entityNode.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
