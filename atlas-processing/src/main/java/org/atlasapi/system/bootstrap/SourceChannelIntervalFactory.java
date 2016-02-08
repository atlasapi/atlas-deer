package org.atlasapi.system.bootstrap;

import org.atlasapi.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import org.joda.time.Interval;

public interface SourceChannelIntervalFactory<T> {

    T create(Publisher source, Channel channel, Interval interval);

}
