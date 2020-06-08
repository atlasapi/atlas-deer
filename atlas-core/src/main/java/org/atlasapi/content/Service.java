package org.atlasapi.content;

public class Service extends Described {

    public static Service copyTo(Service from, Service to) {
        Described.copyTo(from, to);
        return to;
    }

    @Override public <T extends Described> T copyTo(T to) {
        if (to instanceof Service) {
            copyTo(this, (Service) to);
            return to;
        }
        return super.copyTo(to);
    }

    @Override public Service copy() {
        return copyTo(this, new Service());
    }

    @Override
    public Service createNew() {
        return new Service();
    }

}
