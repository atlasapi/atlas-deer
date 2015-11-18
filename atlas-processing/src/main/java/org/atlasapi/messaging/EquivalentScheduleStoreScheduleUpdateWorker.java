package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;


public class EquivalentScheduleStoreScheduleUpdateWorker implements Worker<ScheduleUpdateMessage>{

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentScheduleStoreScheduleUpdateWorker.class);
    
    private final EquivalentScheduleWriter scheduleWriter;
    private final Timer messageTimer;

    public EquivalentScheduleStoreScheduleUpdateWorker(EquivalentScheduleWriter scheduleWriter,
                                                       @Nullable MetricRegistry metrics) {
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer("EquivalentScheduleStoreScheduleUpdateWorker")) : null);
    }

    @Override
    public void process(ScheduleUpdateMessage message) throws RecoverableException {
        LOG.debug("Processing message on id {}, message: {}",
                message.getScheduleUpdate().getSchedule().getChannel(), message);

        try {
            Timer.Context timer = null;
            if (messageTimer != null) {
                timer = messageTimer.time();
            }
            scheduleWriter.updateSchedule(message.getScheduleUpdate());
            if (timer != null) {
                timer.stop();
            }
        } catch (WriteException e) {
            throw new RecoverableException("update failed for " + message.getScheduleUpdate(), e);
        }
    }

}
