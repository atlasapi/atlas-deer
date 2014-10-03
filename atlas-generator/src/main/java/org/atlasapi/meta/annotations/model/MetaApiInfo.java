package org.atlasapi.meta.annotations.model;

import java.util.Set;

import com.google.common.base.Optional;

public interface MetaApiInfo {

	Set<EndPoint> allEndpoints();
	Optional<EndPoint> endpointFor(String key);
}
