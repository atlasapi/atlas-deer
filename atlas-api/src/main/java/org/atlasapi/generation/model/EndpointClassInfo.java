package org.atlasapi.generation.model;

import java.util.Set;

public interface EndpointClassInfo {

    String name();

    String modelKey();

    String description();

    String rootPath();

    Set<Operation> operations();
}
