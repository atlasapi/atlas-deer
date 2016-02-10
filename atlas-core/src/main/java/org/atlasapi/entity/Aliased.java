package org.atlasapi.entity;

import com.google.common.collect.ImmutableSet;

public interface Aliased {

    ImmutableSet<Alias> getAliases();

}
