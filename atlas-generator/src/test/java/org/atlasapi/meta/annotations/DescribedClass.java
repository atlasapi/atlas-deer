package org.atlasapi.meta.annotations;


public class DescribedClass extends IdentifiedClass implements Cloneable {

    @FieldName("description")
    public String getDescription() {
        return "I'm a description";
    }
    
    @FieldName("title")
    public String getTitle() {
        return "I'm a title";
    }
    
}
