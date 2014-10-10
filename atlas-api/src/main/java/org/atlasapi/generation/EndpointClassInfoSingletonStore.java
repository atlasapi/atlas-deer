package org.atlasapi.generation;

import java.util.Map;
import java.util.Set;

import org.atlasapi.generation.model.EndpointClassInfo;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;


public enum EndpointClassInfoSingletonStore implements EndpointClassInfoStore {
    
    INSTANCE;
    
    private static final Logger log = LoggerFactory.getLogger(EndpointClassInfoSingletonStore.class);
    
    private final Map<String, EndpointClassInfo> endpointInfoLookup = Maps.newHashMap();
    
    private EndpointClassInfoSingletonStore() {
        refreshClasses();
    }
    
    private void refreshClasses() {
        try {
            endpointInfoLookup.clear();
            
            Set<Class<? extends EndpointClassInfo>> subTypes = new Reflections("org.atlasapi").getSubTypesOf(EndpointClassInfo.class);
            for (Class<? extends EndpointClassInfo> subType : subTypes) {
                EndpointClassInfo instantiation = subType.newInstance();
                endpointInfoLookup.put(instantiation.name(), instantiation);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            log.error("Error while reflexively obtaining Endpoint Information classes", e);
        }
    }
    
    @Override
    public void register(String key, EndpointClassInfo classInfo) {
        endpointInfoLookup.put(key, classInfo);
    }
    
    @Override
    public Optional<EndpointClassInfo> endpointInfoFor(String key) {
        return Optional.fromNullable(endpointInfoLookup.get(key));
    }

    @Override
    public Set<EndpointClassInfo> allEndpointInformation() {
        return ImmutableSet.copyOf(endpointInfoLookup.values());
    }
}
