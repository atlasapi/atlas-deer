package org.atlasapi.meta.annotations;

import org.atlasapi.meta.annotations.modelprocessing.FieldName;


public class Described extends Identified implements Cloneable {

	/**
     *  some Javadoc-y description of a description field
     * @return
     */
    @FieldName("description")
    public String getDescription() {
        return "I'm a description";
    }
    
    /**
     *  some Javadoc-y description of a title field
     * @return
     */
    @FieldName("title")
    public String getTitle() {
        return "I'm a title";
    }
    
}
