package org.atlasapi;

public class AtlasMain {

    public static void main(String[] args) throws Exception {
        AtlasServer.startWithMonitoringOnPort(8080);
    }

}