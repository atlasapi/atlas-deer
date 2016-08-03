FROM 448613307115.dkr.ecr.us-east-1.amazonaws.com/jvm-oracle8:latest

ENV JETTY_HOME="/usr/local/jetties/atlas" \
    COM_SUN_MANAGEMENT_JMXREMOTE_PORT="19761" \
    COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT="19761" \
    COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE="false" \
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL="false" \
    JAVA_RMI_SERVER_HOSTNAME="127_0.0_1" \
    SUN_NET_INETADDR_TTL="60" \
    MBST_PLATFORM="stage" \
    CASSANDRA_CLIENTTHREADS="50" \
    CASSANDRA_CLUSTER="Stage" \
    CASSANDRA_CONNECTIONSPERHOST_LOCAL="2" \
    CASSANDRA_CONNECTIONSPERHOST_REMOTE="2" \
    CASSANDRA_DATASTAX_TIMEOUTS_CONNECTIONS="1000" \
    CASSANDRA_DATASTAX_TIMEOUTS_READ="11000" \
    CASSANDRA_KEYSPACE="atlas_deer_stage" \
    CASSANDRA_SEEDS="cassandra1.stage.atlas.mbst.tv,cassandra2.stage.atlas.mbst.tv" \
    ELASTICSEARCH_CLUSTER="atlas_deer_stage" \
    ELASTICSEARCH_INDEX="content" \
    ELASTICSEARCH_REQUESTTIMEOUT="60000" \
    ELASTICSEARCH_SEEDS="node1.search.stage.deer.atlas.mbst.tv" \
    GITHUB_AUTH_CONSUMERKEY="" \
    GITHUB_AUTH_CONSUMERSECRET="" \
    GOOGLE_AUTH_CONSUMERKEY="" \
    GOOGLE_AUTH_CONSUMERSECRET="" \
    MESSAGING_BROKER_URL="node1.kafka.mbst.tv:9092,node2.kafka.mbst.tv:9092,node3.kafka.mbst.tv:9092,node4.kafka.mbst.tv:9092" \
    MESSAGING_SYSTEM="AtlasDeerStage" \
    MESSAGING_ZOOKEEPER="node1.kafka.mbst.tv:2181,node2.kafka.mbst.tv:2181,node3.kafka.mbst.tv:2181" \
    MONGO_READ_HOST="db1.owl.atlas.mbst.tv,db3.owl.atlas.mbst.tv" \
    MONGO_READ_NAME="atlas-split" \
    MONGO_WRITE_HOST="db1.stage.atlas.mbst.tv" \
    MONGO_WRITE_NAME="atlas-split" \
    NOTIFICATIONS_EMAIL_FROM="atlas-admin@metabroadcast.com" \
    NOTIFICATIONS_EMAIL_FROMFRIENDLYNAME="Atlas" \
    NOTIFICATIONS_EMAIL_HOST="smtp.gmail.com" \
    NOTIFICATIONS_EMAIL_PASSWORD="" \
    NOTIFICATIONS_EMAIL_TO="atlas@metabroadcast.com" \
    NOTIFICATIONS_EMAIL_USERNAME="atlas-admin@metabroadcast.com" \
    TWITTER_AUTH_CONSUMERKEY="" \
    TWITTER_AUTH_CONSUMERSECRET="" \
    YOUTUBE_CLIENTID="" \
    YOUTUBE_CLIENTSECRET="" \
    YOUTUBE_HANDLING_SERVICE="http://app1.stage.coyote.mbst.tv" \
    JSSE_ENABLESNIEXTENSION="false" \
    OUT_LOG_FILE="/usr/local/jetties/atlas/work/atlas.log" \
    EXT_LOG_FILE="/usr/local/jetties/atlas/work/trace.log" \
    NIMROD_LOG_FILE="/usr/local/jetties/atlas/work/tasks.log" \
    LOG4J_CONFIGURATION="file:////usr/local/jetties/atlas/work/log4j.properties" \
    NIMROD_LOG_LEVEL="INFO" \
    ROOT_LOG_LEVEL="INFO"

COPY atlas-api/target/atlas-api.war /usr/local/jetties/atlas/lib/atlas-api.war
COPY ./log4j.properties /usr/local/jetties/atlas/work/log4j.properties

WORKDIR /usr/local/jetties/atlas

CMD java \
    -Djetty.home="$JETTY_HOME" \
    -server \
    -Dcom.sun.management.jmxremote.port="$COM_SUN_MANAGEMENT_JMXREMOTE_PORT" \
    -Dcom.sun.management.jmxremote.rmi.port="$COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT" \
    -Dcom.sun.management.jmxremote.authenticate="$COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE" \
    -Dcom.sun.management.jmxremote.ssl="$COM_SUN_MANAGEMENT_JMXREMOTE_SSL" \
    -Djava.rmi.server.hostname="$JAVA_RMI_SERVER_HOSTNAME" \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=2000 \
    -Dsun.net.inetaddr.ttl="$SUN_NET_INETADDR_TTL" \
    -DMBST_PLATFORM="$MBST_PLATFORM" \
    -Dcassandra.clientThreads="$CASSANDRA_CLIENTTHREADS" \
    -Dcassandra.cluster="$CASSANDRA_CLUSTER" \
    -Dcassandra.connectionsPerHost.local="$CASSANDRA_CONNECTIONSPERHOST_LOCAL" \
    -Dcassandra.connectionsPerHost.remote="$CASSANDRA_CONNECTIONSPERHOST_REMOTE" \
    -Dcassandra.datastax.timeouts.connections="$CASSANDRA_DATASTAX_TIMEOUTS_CONNECTIONS" \
    -Dcassandra.datastax.timeouts.read="$CASSANDRA_DATASTAX_TIMEOUTS_READ" \
    -Dcassandra.keyspace="$CASSANDRA_KEYSPACE" \
    -Dcassandra.seeds="$CASSANDRA_SEEDS" \
    -Delasticsearch.cluster="$ELASTICSEARCH_CLUSTER" \
    -Delasticsearch.index="$ELASTICSEARCH_INDEX" \
    -Delasticsearch.requestTimeout="$ELASTICSEARCH_REQUESTTIMEOUT" \
    -Delasticsearch.seeds="$ELASTICSEARCH_SEEDS" \
    -Dgithub.auth.consumerKey="$GITHUB_AUTH_CONSUMERKEY" \
    -Dgithub.auth.consumerSecret="$GITHUB_AUTH_CONSUMERSECRET" \
    -Dgoogle.auth.consumerKey="$GOOGLE_AUTH_CONSUMERKEY" \
    -Dgoogle.auth.consumerSecret="$GOOGLE_AUTH_CONSUMERSECRET" \
    -Dmessaging.broker.url="$MESSAGING_BROKER_URL" \
    -Dmessaging.system="$MESSAGING_SYSTEM" \
    -Dmessaging.zookeeper="$MESSAGING_ZOOKEEPER" \
    -Dmongo.read.host="$MONGO_READ_HOST" \
    -Dmongo.read.name="$MONGO_READ_NAME" \
    -Dmongo.write.host="$MONGO_WRITE_HOST" \
    -Dmongo.write.name="$MONGO_WRITE_NAME" \
    -Dnotifications.email.from="$NOTIFICATIONS_EMAIL_FROM" \
    -Dnotifications.email.fromFriendlyName="$NOTIFICATIONS_EMAIL_FROMFRIENDLYNAME" \
    -Dnotifications.email.host="$NOTIFICATIONS_EMAIL_HOST" \
    -Dnotifications.email.password="$NOTIFICATIONS_EMAIL_PASSWORD" \
    -Dnotifications.email.to="$NOTIFICATIONS_EMAIL_TO" \
    -Dnotifications.email.username="$NOTIFICATIONS_EMAIL_USERNAME" \
    -Dtwitter.auth.consumerKey="$TWITTER_AUTH_CONSUMERKEY" \
    -Dtwitter.auth.consumerSecret="$TWITTER_AUTH_CONSUMERSECRET" \
    -Dyoutube.clientId="$YOUTUBE_CLIENTID" \
    -Dyoutube.clientSecret="$YOUTUBE_CLIENTSECRET" \
    -Dyoutube.handling.service="$YOUTUBE_HANDLING_SERVICE" \
    -Djsse.enableSNIExtension="$JSSE_ENABLESNIEXTENSION" \
    -Dout.log.file="$OUT_LOG_FILE" \
    -Dext.log.file="$EXT_LOG_FILE" \
    -Dnimrod.log.file="$NIMROD_LOG_FILE" \
    -Dlog4j.configuration="$LOG4J_CONFIGURATION" \
    -Dnimrod.log.level="$NIMROD_LOG_LEVEL" \
    -Droot.log.level="$ROOT_LOG_LEVEL" \
#    -XX:+PrintGCDetails \
#    -XX:+PrintGCDateStamps \
#    -verbose:gc \
    -XX:+PrintTenuringDistribution \
    -jar lib/atlas-api.war
