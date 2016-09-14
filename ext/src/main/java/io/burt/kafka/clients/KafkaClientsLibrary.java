package io.burt.kafka.clients;

import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.exceptions.RaiseException;
import org.jruby.RubyClass;
import org.jruby.runtime.load.Library;

public class KafkaClientsLibrary implements Library {
  public void load(Ruby runtime, boolean wrap) {
    RubyModule kafkaClientsModule = KafkaClients.install(runtime);
    Producer.install(runtime, kafkaClientsModule);
    FutureWrapper.install(runtime, kafkaClientsModule);
    RecordMetadataWrapper.install(runtime, kafkaClientsModule);
    PartitionInfoWrapper.install(runtime, kafkaClientsModule);
    NodeWrapper.install(runtime, kafkaClientsModule);
    ClusterWrapper.install(runtime, kafkaClientsModule);
    TopicPartitionWrapper.install(runtime, kafkaClientsModule);
    installErrors(runtime, kafkaClientsModule);
  }

  private void installErrors(Ruby runtime, RubyModule parentModule) {
    RubyClass standardErrorClass = runtime.getStandardError();
    RubyClass kafkaErrorClass = parentModule.defineClassUnder("KafkaError", standardErrorClass, standardErrorClass.getAllocator());
    RubyClass apiExceptionClass = parentModule.defineClassUnder("ApiError", kafkaErrorClass, standardErrorClass.getAllocator());
    parentModule.defineClassUnder("ConfigError", apiExceptionClass, standardErrorClass.getAllocator());
    parentModule.defineClassUnder("RecordTooLargeError", apiExceptionClass, standardErrorClass.getAllocator());
    parentModule.defineClassUnder("TimeoutError", kafkaErrorClass, standardErrorClass.getAllocator());
  }

  static RubyClass mapErrorClass(Ruby runtime, Throwable t) {
    String type = "StandardError";
    if (t instanceof ConfigException) {
      type = "Kafka::Clients::ConfigError";
    } else if (t instanceof RecordTooLargeException) {
      type = "Kafka::Clients::RecordTooLargeError";
    } else if (t instanceof ApiException) {
      type = "Kafka::Clients::ApiError";
    } else if (t instanceof KafkaException) {
      type = "Kafka::Clients::KafkaError";
    } else if (t instanceof TimeoutException) {
      type = "Kafka::Clients::TimeoutError";
    }
    return (RubyClass) runtime.getClassFromPath(type);
  }

  static RaiseException newRaiseException(Ruby runtime, Throwable t) {
    return runtime.newRaiseException(mapErrorClass(runtime, t), t.getMessage());
  }
}
