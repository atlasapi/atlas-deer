package org.atlasapi.attribute;


public class TestClass extends ParentTestClass {

    @AtlasAttribute("foo")
    public String getFoo() {
        return "FOO";
    }
    
    @AtlasAttribute("goo")
    public String getGoo() {
        return "GOO";
    }
    
}
