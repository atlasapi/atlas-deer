package org.atlasapi.system.health.probes;

import com.google.api.client.util.Lists;
import com.metabroadcast.common.health.probes.Probe;
import com.metabroadcast.common.health.probes.ProbeResult;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;

import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

public class AstyanaxProbe implements Probe {

    private final AstyanaxContext<Keyspace> context;

    private AstyanaxProbe(AstyanaxContext<Keyspace> context) {
        this.context = checkNotNull(context);
    }

    public static AstyanaxProbe create(AstyanaxContext<Keyspace> context) {
        return new AstyanaxProbe(context);
    }

    @Override
    public Callable<ProbeResult> createRequest() {
        return () -> {
            try {
                ConnectionPoolMonitor pool = context.getConnectionPoolMonitor();
                List<String> infoList = Lists.newArrayList();

                infoList.add("Socket timeouts: " + Long.toString(pool.getSocketTimeoutCount()));
                infoList.add("Transport errors: " + Long.toString(pool.getTransportErrorCount()));
                infoList.add("Pool-exhausted timeouts: "
                        + Long.toString(pool.getPoolExhaustedTimeoutCount()));
                infoList.add("Connections opened: "
                        + Long.toString(pool.getConnectionCreatedCount()));
                infoList.add("Connections closed: "
                        + Long.toString(pool.getConnectionClosedCount()));

                infoList.add("Active hosts: " + Long.toString(pool.getHostActiveCount()));

                if (pool.getHostActiveCount() <= 0) {
                    return ProbeResult.unhealthy("0 active hosts");
                }

                return ProbeResult.healthy();
            } catch (Exception e) {
                return ProbeResult.unhealthy(e);
            }
        };
    }

}
