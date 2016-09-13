package io.burt.kafka.clients;

import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.RubyClass;
import org.jruby.runtime.load.Library;

public class KafkaClientsLibrary implements Library {
  public void load(Ruby runtime, boolean wrap) {
    RubyModule kafkaClientsModule = KafkaClients.install(runtime);
    Producer.install(runtime, kafkaClientsModule);
    FutureWrapper.install(runtime, kafkaClientsModule);
    installErrors(runtime, kafkaClientsModule);
  }

  private void installErrors(Ruby runtime, RubyModule parentModule) {
    RubyClass standardErrorClass = runtime.getStandardError();
    RubyClass kafkaErrorClass = parentModule.defineClassUnder("KafkaError", standardErrorClass, standardErrorClass.getAllocator());
    parentModule.defineClassUnder("ConfigError", kafkaErrorClass, standardErrorClass.getAllocator());
    parentModule.defineClassUnder("TimeoutError", kafkaErrorClass, standardErrorClass.getAllocator());
  }
}
