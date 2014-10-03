package org.atlasapi.meta.annotations;

import org.atlasapi.meta.annotations.modelprocessing.FieldName;


public class Identified {

	// no javadoc
    @FieldName("id")
    public String getId() {
        return "I'm an ID";
    }
    
}
