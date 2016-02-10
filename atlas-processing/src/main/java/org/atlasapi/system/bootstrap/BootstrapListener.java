package org.atlasapi.system.bootstrap;

public interface BootstrapListener<T> {

    void beforeChange();

    void onChange(Iterable<? extends T> changed);

    void afterChange();
}
