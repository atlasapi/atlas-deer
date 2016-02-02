package org.atlasapi.system.health;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;

import static com.google.common.base.Preconditions.checkNotNull;

public class AstyanaxProbe implements HealthProbe {

    private final AstyanaxContext<Keyspace> context;

    public AstyanaxProbe(AstyanaxContext<Keyspace> context) {
        this.context = checkNotNull(context);
    }

    @Override
    public ProbeResult probe() throws Exception {
        ProbeResult result = new ProbeResult(title());
        ConnectionPoolMonitor pool = context.getConnectionPoolMonitor();

        result.addInfo("Socket timeouts", Long.toString(pool.getSocketTimeoutCount()));
        result.addInfo("Transport errors", Long.toString(pool.getTransportErrorCount()));
        result.addInfo(
                "Pool-exhausted timeouts",
                Long.toString(pool.getPoolExhaustedTimeoutCount())
        );
        result.addInfo("Connections opened", Long.toString(pool.getConnectionCreatedCount()));
        result.addInfo("Connections closed", Long.toString(pool.getConnectionClosedCount()));

        result.add(
                "Active hosts",
                Long.toString(pool.getHostActiveCount()),
                pool.getHostActiveCount() > 0
        );

        return result;
    }

    @Override
    public String title() {
        return "Astyanax";
    }

    @Override
    public String slug() {
        return "astyanax";
    }
}
