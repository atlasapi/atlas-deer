package org.atlasapi.content;

public class Service extends Described {

    @Override
    public Service copy() {
        Service service = new Service();
        copyTo(this, service);
        return service;
    }

}
