package io.burt.kafka.clients;

import org.apache.kafka.common.errors.ApiException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.anno.JRubyModule;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.Arrays;
import java.util.List;

@JRubyModule(name = "Kafka::Clients")
public class KafkaClients {
  static RubyModule install(Ruby runtime) {
    RubyModule kafkaModule = runtime.defineModule("Kafka");
    RubyModule kafkaClientsModule = kafkaModule.defineModuleUnder("Clients");
    kafkaClientsModule.defineAnnotatedMethods(KafkaClients.class);
    return kafkaClientsModule;
  }

  private static final List<String> ERROR_PACKAGES = Arrays.asList(
    "org.apache.kafka.common.errors",
    "org.apache.kafka.clients.consumer",
    "org.apache.kafka.clients.producer",
    "org.apache.kafka.common.config"
  );

  @JRubyMethod(name = "const_missing", meta = true)
  public static IRubyObject constMissing(ThreadContext ctx, IRubyObject self, IRubyObject name) {
    RubyModule module = ctx.runtime.getClassFromPath("Kafka::Clients");
    String rubyName = name.toString();
    if (rubyName.endsWith("Error")) {
      String javaName = rubyName.substring(0, rubyName.length() - 5) + "Exception";
      for (String packageName : ERROR_PACKAGES) {
        try {
          Class<?> exceptionClass = Class.forName(String.format("%s.%s", packageName, javaName));
          String baseClassName = "Kafka::Clients::KafkaError";
          if (ApiException.class.isAssignableFrom(exceptionClass)) {
            baseClassName = "Kafka::Clients::ApiError";
          }
          return module.defineClassUnder(rubyName, (RubyClass) ctx.runtime.getClassFromPath(baseClassName), ctx.runtime.getStandardError().getAllocator());
        } catch (ClassNotFoundException cce) {
          continue;
        }
      }
    }
    return module.const_missing(ctx, name, Block.NULL_BLOCK);
  }
}
