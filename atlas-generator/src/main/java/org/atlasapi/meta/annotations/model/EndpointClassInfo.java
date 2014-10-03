package org.atlasapi.meta.annotations.model;

import java.util.Set;

public interface EndpointClassInfo {

	String name();
	String description();
	String rootPath();
	Set<Operation> operations();
	Class<?> returnedType();
}
