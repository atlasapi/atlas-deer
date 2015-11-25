package org.atlasapi.system;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;

@Configuration
public class ApiMetricsModule implements MetricsModule {

    @Override public @Bean MetricRegistry metrics() {
        return new MetricRegistry();
    }
}
