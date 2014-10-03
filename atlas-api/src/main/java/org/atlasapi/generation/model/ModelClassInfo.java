package org.atlasapi.generation.model;

import java.util.Set;

public interface ModelClassInfo {

	String name();
	String description();
	Set<FieldInfo> fields();
	Class<?> describedType();
}
