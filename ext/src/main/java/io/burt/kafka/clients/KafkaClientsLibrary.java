package io.burt.kafka.clients;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.KafkaException;

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
    parentModule.defineClassUnder("ApiError", kafkaErrorClass, standardErrorClass.getAllocator());
  }

  static RubyClass mapErrorClass(Ruby runtime, Throwable t) {
    if (t instanceof KafkaException) {
      String javaName = t.getClass().getSimpleName();
      String rubyName = javaName.substring(0, javaName.length() - 9) + "Error";
      return (RubyClass) runtime.getClassFromPath(String.format("Kafka::Clients::%s", rubyName));
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
          kafkaConfig.put("bootstrap.servers", valueString);
        } else if (keyStr.equals("group_id")) {
          kafkaConfig.put("group.id", value.asString().asJavaString());
        } else if (keyStr.equals("partitioner")) {
          kafkaConfig.put("partitioner.class", "io.burt.kafka.clients.PartitionerProxy");
          kafkaConfig.put("io.burt.kafka.clients.partitioner", value);
        } else if (keyStr.equals("acks")) {
          kafkaConfig.put("acks", value.asString().asJavaString());
        } else if (keyStr.equals("compression_type")) {
          kafkaConfig.put("compression.type", value.convertToString().asJavaString());
        } else if (keyStr.equals("retries")) {
          kafkaConfig.put("retries", (int) value.convertToInteger().getLongValue());
        } else if (keyStr.equals("batch_size")) {
          kafkaConfig.put("batch.size", (int) value.convertToInteger().getLongValue());
        } else if (keyStr.equals("client_id")) {
          kafkaConfig.put("client.id", value.asString().asJavaString());
        } else if (keyStr.equals("linger")) {
          kafkaConfig.put("linger.ms", (int) (value.convertToFloat().getDoubleValue() * 1000));
        } else if (keyStr.equals("max_block")) {
          kafkaConfig.put("max.block.ms", (int) (value.convertToFloat().getDoubleValue() * 1000));
        } else if (keyStr.equals("max_request_size")) {
          kafkaConfig.put("max.request.size", (int) value.convertToInteger().getLongValue());
        } else if (keyStr.equals("max_poll_records")) {
          kafkaConfig.put("max.poll.records", (int) value.convertToInteger().getLongValue());
        } else if (keyStr.equals("request_timeout")) {
          kafkaConfig.put("request.timeout.ms", (int) (value.convertToFloat().getDoubleValue() * 1000));
        } else if (keyStr.equals("auto_commit")) {
          kafkaConfig.put("enable.auto.commit", value.isTrue());
        } else if (keyStr.equals("auto_commit_interval")) {
          kafkaConfig.put("auto.commit.interval.ms", (int) (value.convertToFloat().getDoubleValue() * 1000));
        } else if (keyStr.equals("auto_offset_reset")) {
          kafkaConfig.put("auto.offset.reset", value.convertToString().asJavaString());
        }
      } else if (!value.isNil()) {
        kafkaConfig.put(key.asJavaString(), value.asString().asJavaString());
      }
    }
    return kafkaConfig;
  }
}
