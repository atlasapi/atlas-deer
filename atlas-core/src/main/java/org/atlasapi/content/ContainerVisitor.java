package org.atlasapi.content;

public interface ContainerVisitor<VALUE> {

    VALUE visit(Brand brand);

    VALUE visit(Series series);

}
