package org.atlasapi.entity;

import org.atlasapi.meta.annotations.FieldName;

import com.google.common.collect.ImmutableSet;

public interface Aliased {

	@FieldName("aliases")
    ImmutableSet<Alias> getAliases();
    
}
