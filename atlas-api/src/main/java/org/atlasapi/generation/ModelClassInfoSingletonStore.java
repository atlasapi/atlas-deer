package org.atlasapi.generation;

import java.util.Map;
import java.util.Set;

import org.atlasapi.generation.model.ModelClassInfo;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;


public enum ModelClassInfoSingletonStore implements ModelClassInfoStore {
    
    INSTANCE;
    
    private static final Logger log = LoggerFactory.getLogger(ModelClassInfoSingletonStore.class);
    
    private final Map<String, ModelClassInfo> modelInfoLookup = Maps.newHashMap();
    
    private ModelClassInfoSingletonStore() {
        refreshClasses();
    }
    
    private void refreshClasses() {
        try {
            modelInfoLookup.clear();
            
            Set<Class<? extends ModelClassInfo>> subTypes = new Reflections("org.atlasapi").getSubTypesOf(ModelClassInfo.class);
            for (Class<? extends ModelClassInfo> subType : subTypes) {
                ModelClassInfo instantiation = subType.newInstance();
                modelInfoLookup.put(instantiation.key(), instantiation);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            //TODO THROOOOOOOOWWWWW
            log.error("Error while reflexively obtaining Model Information classes", e);
        }
    }
    
    @Override
    public void register(String key, ModelClassInfo classInfo) {
        modelInfoLookup.put(key, classInfo);
    }
    
    @Override
    public Optional<ModelClassInfo> classInfoFor(String key) {
        return Optional.fromNullable(modelInfoLookup.get(key));
    }

    @Override
    public Set<ModelClassInfo> allClassInformation() {
        return ImmutableSet.copyOf(modelInfoLookup.values());
    }
}
