package org.atlasapi.generation;

import java.util.Set;

import org.atlasapi.generation.model.EndpointClassInfo;

import com.google.common.base.Optional;

public interface EndpointClassInfoStore {

    void register(String key, EndpointClassInfo endpointInfo);

    Optional<EndpointClassInfo> endpointInfoFor(String key);

    Set<EndpointClassInfo> allEndpointInformation();
}
