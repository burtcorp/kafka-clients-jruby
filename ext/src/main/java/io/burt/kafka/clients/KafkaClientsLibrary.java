package io.burt.kafka.clients;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import java.util.concurrent.TimeoutException;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubySymbol;
import org.jruby.exceptions.RaiseException;
import org.jruby.RubyClass;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;

public class KafkaClientsLibrary implements Library {
  static final String DESERIALIZER_CONFIG_PREFIX = "io.burt.kafka.clients.deserializer.";
  static final String VALUE_DESERIALIZER_CONFIG = DESERIALIZER_CONFIG_PREFIX + "value";
  static final String KEY_DESERIALIZER_CONFIG = DESERIALIZER_CONFIG_PREFIX + "key";
  static final String SERIALIZER_CONFIG_PREFIX = "io.burt.kafka.clients.serializer.";
  static final String VALUE_SERIALIZER_CONFIG = SERIALIZER_CONFIG_PREFIX + "value";
  static final String KEY_SERIALIZER_CONFIG = SERIALIZER_CONFIG_PREFIX + "key";
  static final String PARTITIONER_CONFIG = "io.burt.kafka.clients.partitioner";
  static final String RUNTIME_CONFIG = "io.burt.kafka.clients.runtime";
  static final String ENCODING_CONFIG = "io.burt.kafka.clients.encoding";

  public void load(Ruby runtime, boolean wrap) {
    RubyModule kafkaClientsModule = KafkaClients.install(runtime);
    ProducerWrapper.install(runtime, kafkaClientsModule);
    FutureWrapper.install(runtime, kafkaClientsModule);
    RecordMetadataWrapper.install(runtime, kafkaClientsModule);
    PartitionInfoWrapper.install(runtime, kafkaClientsModule);
    NodeWrapper.install(runtime, kafkaClientsModule);
    ClusterWrapper.install(runtime, kafkaClientsModule);
    TopicPartitionWrapper.install(runtime, kafkaClientsModule);
    ProducerRecordWrapper.install(runtime, kafkaClientsModule);
    ConsumerWrapper.install(runtime, kafkaClientsModule);
    ConsumerRecordsWrapper.install(runtime, kafkaClientsModule);
    OffsetAndMetadataWrapper.install(runtime, kafkaClientsModule);
    ConsumerRecordWrapper.install(runtime, kafkaClientsModule);
    installErrors(runtime, kafkaClientsModule);
  }

  private void installErrors(Ruby runtime, RubyModule parentModule) {
    RubyClass standardErrorClass = runtime.getStandardError();
    RubyClass kafkaErrorClass = parentModule.defineClassUnder("KafkaError", standardErrorClass, standardErrorClass.getAllocator());
    RubyClass apiErrorClass = parentModule.defineClassUnder("ApiError", kafkaErrorClass, standardErrorClass.getAllocator());
    parentModule.defineClassUnder("RetriableError", apiErrorClass, standardErrorClass.getAllocator());
  }

  static RubyClass mapErrorClass(Ruby runtime, Throwable t) {
    if (t instanceof KafkaException) {
      String javaName = t.getClass().getSimpleName();
      String rubyName = javaName.substring(0, javaName.length() - 9) + "Error";
      return (RubyClass) runtime.getClassFromPath(String.format("Kafka::Clients::%s", rubyName));
    } else if (t instanceof TimeoutException) {
      return (RubyClass) runtime.getModule("Timeout").getConstant("Error");
    } else {
      return runtime.getStandardError();
    }
  }

  static RaiseException newRaiseException(Ruby runtime, Throwable t) {
    return runtime.newRaiseException(mapErrorClass(runtime, t), t.getMessage());
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> toKafkaConfiguration(RubyHash config) {
    Map<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, RubyStringSerializer.class);
    kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RubyStringSerializer.class);
    kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, RubyStringDeserializer.class);
    kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RubyStringDeserializer.class);
    for (IRubyObject key : (List<IRubyObject>) config.keys().getList()) {
      IRubyObject value = config.fastARef(key);
      if (key instanceof RubySymbol && !value.isNil()) {
        String keyStr = key.asJavaString();
        if (keyStr.equals("bootstrap_servers")) {
          String valueString;
          if (value instanceof RubyArray) {
            Ruby runtime = config.getRuntime();
            valueString = value.convertToArray().join(runtime.getCurrentContext(), runtime.newString(",")).asJavaString();
          } else {
            valueString = value.asString().asJavaString();
          }
          kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, valueString);
        } else if (keyStr.equals("group_id")) {
          kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, value.asString().asJavaString());
        } else if (keyStr.equals("partitioner")) {
          kafkaConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PartitionerProxy.class);
          kafkaConfig.put(PARTITIONER_CONFIG, value);
        } else if (keyStr.equals("key_serializer")) {
          kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SerializerProxy.class);
          kafkaConfig.put(KEY_SERIALIZER_CONFIG, value);
        } else if (keyStr.equals("value_serializer")) {
          kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SerializerProxy.class);
          kafkaConfig.put(VALUE_SERIALIZER_CONFIG, value);
        } else if (keyStr.equals("key_deserializer")) {
          kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DeserializerProxy.class);
          kafkaConfig.put(KEY_DESERIALIZER_CONFIG, value);
        } else if (keyStr.equals("value_deserializer")) {
          kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DeserializerProxy.class);
          kafkaConfig.put(VALUE_DESERIALIZER_CONFIG, value);
        } else if (keyStr.equals("acks")) {
          kafkaConfig.put(ProducerConfig.ACKS_CONFIG, value.asString().asJavaString());
        } else if (keyStr.equals("compression_type")) {
          kafkaConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, value.convertToString().asJavaString());
        } else if (keyStr.equals("retries")) {
          kafkaConfig.put(ProducerConfig.RETRIES_CONFIG, (int) value.convertToInteger().getLongValue());
        } else if (keyStr.equals("batch_size")) {
          kafkaConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, (int) value.convertToInteger().getLongValue());
        } else if (keyStr.equals("client_id")) {
          kafkaConfig.put(ProducerConfig.CLIENT_ID_CONFIG, value.asString().asJavaString());
        } else if (keyStr.equals("linger")) {
          kafkaConfig.put(ProducerConfig.LINGER_MS_CONFIG, (int) (value.convertToFloat().getDoubleValue() * 1000));
        } else if (keyStr.equals("max_block")) {
          kafkaConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int) (value.convertToFloat().getDoubleValue() * 1000));
        } else if (keyStr.equals("max_request_size")) {
          kafkaConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, (int) value.convertToInteger().getLongValue());
        } else if (keyStr.equals("max_poll_records")) {
          kafkaConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, (int) value.convertToInteger().getLongValue());
        } else if (keyStr.equals("request_timeout")) {
          kafkaConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) (value.convertToFloat().getDoubleValue() * 1000));
        } else if (keyStr.equals("auto_commit")) {
          kafkaConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, value.isTrue());
        } else if (keyStr.equals("auto_commit_interval")) {
          kafkaConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, (int) (value.convertToFloat().getDoubleValue() * 1000));
        } else if (keyStr.equals("auto_offset_reset")) {
          kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value.convertToString().asJavaString());
        } else if (keyStr.equals("encoding")) {
          kafkaConfig.put(ENCODING_CONFIG, value);
        }
      } else if (!value.isNil()) {
        String valueString;
        if (value instanceof RubyArray) {
          Ruby runtime = config.getRuntime();
          valueString = value.convertToArray().join(runtime.getCurrentContext(), runtime.newString(",")).asJavaString();
        } else {
          valueString = value.asString().asJavaString();
        }
        kafkaConfig.put(key.asJavaString(), valueString);
      }
    }
    kafkaConfig.put(RUNTIME_CONFIG, config.getRuntime());
    return kafkaConfig;
  }
}
