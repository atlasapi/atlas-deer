package org.atlasapi.generation.model;

import static com.google.common.base.Preconditions.checkNotNull;


public enum JsonType {

    NUMBER("Number"),
    STRING("String"),
    BOOLEAN("Boolean"),
    ARRAY("Array"),
    OBJECT("Object"),
    ;
    
    private final String value;
    
    private JsonType(String value) {
        this.value = checkNotNull(value);
    }
    
    public String value() {
        return value;
    }
}
