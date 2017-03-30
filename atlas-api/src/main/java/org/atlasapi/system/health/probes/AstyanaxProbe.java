package org.atlasapi.system.health.probes;

import com.google.api.client.util.Lists;
import com.metabroadcast.common.health.probes.Probe;
import com.metabroadcast.common.health.probes.ProbeResult;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import joptsimple.internal.Strings;

import java.util.List;
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

                if (isUnhealthy(pool)) {
                    return ProbeResult.unhealthy(identifier, Strings.join(infoList, ", "));
                }

                return ProbeResult.healthy(identifier);
            } catch (Exception e) {
                return ProbeResult.unhealthy(identifier, e);
            }
        };
    }

    private boolean isUnhealthy(ConnectionPoolMonitor poolMonitor) {
        return poolMonitor.getHostActiveCount() <= 0;
    }

}
