package org.atlasapi.system;

import com.codahale.metrics.MetricRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsModule {

    public
    @Bean
    MetricRegistry metrics() {
        return new MetricRegistry();
    }
}
