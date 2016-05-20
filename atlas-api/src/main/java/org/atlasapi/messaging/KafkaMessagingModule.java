package org.atlasapi.messaging;

import com.metabroadcast.common.queue.MessageConsumerFactory;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.queue.kafka.KafkaMessageConsumerFactory;
import com.metabroadcast.common.queue.kafka.KafkaMessageSenderFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaMessagingModule implements MessagingModule {

    @Value("${messaging.broker.url}")
    private String brokerUrl;
    @Value("${messaging.zookeeper}")
    private String zookeeper;
    @Value("${messaging.system}")
    private String messagingSystem;

    @Override
    @Bean
    public MessageSenderFactory messageSenderFactory() {
        return new KafkaMessageSenderFactory(brokerUrl, messagingSystem);
    }

    @Override
    @Bean
    public MessageConsumerFactory<KafkaConsumer> messageConsumerFactory() {
        return KafkaMessageConsumerFactory.create(zookeeper, messagingSystem);
    }
}
