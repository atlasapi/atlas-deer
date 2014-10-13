package org.atlasapi.query.v4.meta;

import static com.google.common.base.Preconditions.checkNotNull;


public class MetaApiLinkCreator implements LinkCreator {
    
    private final String atlasUriBase;
    
    public MetaApiLinkCreator(String atlasUriBase) {
        this.atlasUriBase = checkNotNull(atlasUriBase);
    }

    @Override
    public String createModelLink(String type) {
        StringBuilder modelLink = new StringBuilder(); 
        
//        String platform = Configurer.getPlatform();
//        // TODO this is a MASSIVELY ENORMOUS HACK, but along the right lines
//        modelLink.append("http://");
//        if ("STAGE".equalsIgnoreCase(platform)) {
//            modelLink.append("stage.atlas.metabroadcast.com");
//        } else if ("PROD".equalsIgnoreCase(platform)) {
//            modelLink.append("atlas.metabroadcast.com");
//        } else {
//            modelLink.append("dev.mbst.tv:8080");
//        }
        modelLink.append(atlasUriBase);
        modelLink.append("/4/model_classes/"); 
        modelLink.append(type);
        modelLink.append(".json");
        
        return modelLink.toString();
    }
}
