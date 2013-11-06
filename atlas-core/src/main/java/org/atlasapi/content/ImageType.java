package org.atlasapi.content;

public enum ImageType {

    PRIMARY("primary"),
    ADDITIONAL("additional"),
    BOX_ART("box_art"),
    POSTER("poster"),
    LOGO("logo");
    
    private final String name;
    
    private ImageType(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
}
