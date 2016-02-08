package org.atlasapi.generation;

import org.atlasapi.meta.annotations.FieldName;

public class MidLevelClass extends TopLevelClass {

    @FieldName("mid")
    public String getMid() {
        return "mid";
    }
}
