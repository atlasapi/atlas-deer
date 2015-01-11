package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.queue.Worker;

import javax.annotation.Nullable;


public class EquivalentScheduleStoreScheduleUpdateWorker implements Worker<ScheduleUpdateMessage>{

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final EquivalentScheduleWriter scheduleWriter;
    private final Timer messageTimer;

    public EquivalentScheduleStoreScheduleUpdateWorker(EquivalentScheduleWriter scheduleWriter,
                                                       @Nullable MetricRegistry metrics) {
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer("EquivalentScheduleStoreScheduleUpdateWorker")) : null);
    }

    @Override
    public void process(ScheduleUpdateMessage message) {
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
            log.error("update failed for " + message.getScheduleUpdate(), e);
        }
    }

}
