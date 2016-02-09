package org.atlasapi.generation;

import org.atlasapi.meta.annotations.FieldName;

public class BottomLevelClass extends TopLevelClass {

    @FieldName("bottom")
    public String getBottom() {
        return "bottom";
    }
}
