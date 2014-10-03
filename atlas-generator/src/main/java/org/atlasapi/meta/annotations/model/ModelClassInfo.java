package org.atlasapi.meta.annotations.model;

import java.util.Set;

public interface ModelClassInfo {

	String name();
	String description();
	Set<FieldInfo> fields();
}
