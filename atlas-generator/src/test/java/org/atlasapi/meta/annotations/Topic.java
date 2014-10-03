package org.atlasapi.meta.annotations;

import java.util.Set;

import org.atlasapi.meta.annotations.modelprocessing.FieldName;

import com.google.common.collect.ImmutableSet;

public class Topic {
	
    /**
     *  some Javadoc-y description of a namespace field
     * @return
     */
    @FieldName("namespace")
    public String getNamespace() {
        return "I'm a namespace";
    }
    
    /**
     *  this is a test of sets
     * @return
     */
    @FieldName("aliases")
    public Set<String> getAliases() {
        return ImmutableSet.of("alias1", "alias2");
    }
    
    /**
     *  this is a test of complex embedded types
     * @return
     */
    @FieldName("content")
    public Set<Content> getContent() {
        return ImmutableSet.of(new Content());
    }
}
