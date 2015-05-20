package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.metabroadcast.common.queue.RecoverableException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.queue.Worker;

import javax.annotation.Nullable;

public class EquivalentScheduleStoreGraphUpdateWorker implements Worker<EquivalenceGraphUpdateMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final EquivalentScheduleWriter scheduleWriter;
    private final Timer messageTimer;

    public EquivalentScheduleStoreGraphUpdateWorker(EquivalentScheduleWriter scheduleWriter,
                                                    @Nullable MetricRegistry metrics) {
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer("EquivalentScheduleStoreGraphUpdateWorker")) : null);
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) throws RecoverableException {
        try {
            Timer.Context timer = null;
            if (messageTimer != null) {
                timer = messageTimer.time();
            }
            scheduleWriter.updateEquivalences(message.getGraphUpdate());
            if (timer != null) {
                timer.stop();
            }
        } catch (WriteException e) {
            throw new RecoverableException("update failed for " + message.getGraphUpdate(), e);
        }
    }

}
