package org.atlasapi.meta.annotations;

import org.atlasapi.meta.annotations.modelprocessing.FieldName;

public class Content extends Described {

    
    /**
     *  some Javadoc-y description of a language field
     * @return
     */
    @FieldName("language")
    public String getLanguage() {
        return "I'm a language";
    }
}
