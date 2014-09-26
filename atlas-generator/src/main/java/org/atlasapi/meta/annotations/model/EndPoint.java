package org.atlasapi.meta.annotations.model;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class EndPoint {
	
	private final Set<ValueField> fields;
	
	public EndPoint(Iterable<ValueField> fields) {
		this.fields = ImmutableSet.copyOf(fields);
	}
	
	public Set<ValueField> fields() {
		return fields;
	}
}
