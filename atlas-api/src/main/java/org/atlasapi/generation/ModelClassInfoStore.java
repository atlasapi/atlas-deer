package org.atlasapi.generation;

import java.util.Set;

import org.atlasapi.generation.model.ModelClassInfo;

import com.google.common.base.Optional;


public interface ModelClassInfoStore {

    void register(String key, ModelClassInfo classInfo);
    Optional<ModelClassInfo> classInfoFor(String key);
    Set<ModelClassInfo> allClassInformation();
}
