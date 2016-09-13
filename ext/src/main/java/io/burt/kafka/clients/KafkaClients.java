package io.burt.kafka.clients;

import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyModule;

@JRubyModule(name = "Kafka::Clients")
public class KafkaClients {
  static RubyModule install(Ruby runtime) {
    RubyModule kafkaModule = runtime.defineModule("Kafka");
    RubyModule kafkaClientsModule = kafkaModule.defineModuleUnder("Clients");
    kafkaClientsModule.defineAnnotatedMethods(KafkaClients.class);
    return kafkaClientsModule;
  }
}
