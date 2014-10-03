package org.atlasapi.meta.annotations.model;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class EndPoint {
	
	private final String name;
	private final String description;
	private final Set<Operation> operations;
	private final Set<FieldInfo> fields;
	
	public EndPoint(String name, String description, Iterable<Operation> operations, Iterable<FieldInfo> fields) {
		this.name = checkNotNull(name);
		this.description = checkNotNull(description);
		this.operations = ImmutableSet.copyOf(operations);
		this.fields = ImmutableSet.copyOf(fields);
	}
	
	public String name() {
		return name;
	}
	
	public String description() {
		return description;
	}
	
	public Set<Operation> operations() {
		return operations;
	}
	
	public Set<FieldInfo> fields() {
		return fields;
	}
}
