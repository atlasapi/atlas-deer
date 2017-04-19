package org.atlasapi.system.health.probes;

import com.metabroadcast.common.health.probes.Probe;
import com.metabroadcast.common.health.probes.ProbeResult;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;

import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

public class AstyanaxProbe extends Probe {

    private final AstyanaxContext<Keyspace> context;

    private AstyanaxProbe(String identifier, AstyanaxContext<Keyspace> context) {
        super(identifier);
        this.context = checkNotNull(context);
    }

    public static AstyanaxProbe create(String identifier, AstyanaxContext<Keyspace> context) {
        return new AstyanaxProbe(identifier, context);
    }

    @Override
    public Callable<ProbeResult> createRequest() {
        return () -> {
            try {
                ConnectionPoolMonitor pool = context.getConnectionPoolMonitor();

                if (pool.getHostActiveCount() <= 0) {
                    return ProbeResult.unhealthy(identifier, "No active hosts");
                }

                return ProbeResult.healthy(identifier);
            } catch (Exception e) {
                return ProbeResult.unhealthy(identifier, e);
            }
        };
    }

}
