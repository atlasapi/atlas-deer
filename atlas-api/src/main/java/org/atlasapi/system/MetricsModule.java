package org.atlasapi.system;

import org.springframework.context.annotation.Bean;

import com.codahale.metrics.MetricRegistry;

public interface MetricsModule {

    @Bean MetricRegistry metrics();
}
