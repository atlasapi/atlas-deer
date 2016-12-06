package org.atlasapi.system;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.metabroadcast.common.properties.Configurer;

import com.codahale.metrics.Clock;
import com.codahale.metrics.JvmAttributeGaugeSet;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SuppressWarnings("PublicConstructor")
@Configuration
public class MetricsModule {

    private final String environmentPrefix = Configurer.get("metrics.environment.prefix").get();
    private final String graphiteHost = Configurer.get("metrics.graphite.host").get();
    private final int graphitePort = Configurer.get("metrics.graphite.port").toInt();

    @Bean
    public MetricRegistry metrics() {
        MetricRegistry metrics = new MetricRegistry();

        registerMetrics("gc.", new GarbageCollectorMetricSet(), metrics);
        registerMetrics("memory.", new MemoryUsageGaugeSet(), metrics);
        registerMetrics("threads.", new ThreadStatesGaugeSet(), metrics);
        registerMetrics("jvm.", new JvmAttributeGaugeSet(), metrics);

        startGraphiteReporter(metrics);

        return metrics;
    }

    @Bean
    public MetricsController metricsController() {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        collectorRegistry.register(new DropwizardExports(metrics()));

        return MetricsController.create(collectorRegistry);
    }

    private void registerMetrics(String prefix, MetricSet metrics, MetricRegistry registry) {
        for (Map.Entry<String, Metric> metric : metrics.getMetrics().entrySet()) {
            registry.register(prefix.concat(metric.getKey()), metric.getValue());
        }
    }

    private void startGraphiteReporter(MetricRegistry metrics) {
        GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                .prefixedWith("atlas.deer." + environmentPrefix + ".")
                .withClock(new Clock.UserTimeClock())
                .build(new Graphite(graphiteHost, graphitePort));

        reporter.start(1, TimeUnit.MINUTES);
    }
}
