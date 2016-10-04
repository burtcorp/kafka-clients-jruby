package io.burt.kafka.clients;

import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.WakeupException;

import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.exceptions.RaiseException;
import org.jruby.RubyClass;
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
}
