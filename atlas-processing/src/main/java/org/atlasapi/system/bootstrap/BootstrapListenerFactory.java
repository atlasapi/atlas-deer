package org.atlasapi.system.bootstrap;

public interface BootstrapListenerFactory<T> {

    BootstrapListener<T> buildWithConcurrency(int concurrencyLevel);

}
