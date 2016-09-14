package io.burt.kafka.clients;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@SuppressWarnings("serial")
@JRubyClass(name = "Kafka::Clients::Consumer")
public class ConsumerWrapper extends RubyObject {
  private Consumer<IRubyObject, IRubyObject> kafkaConsumer;

  public ConsumerWrapper(Ruby runtime, RubyClass metaClass) {
    super(runtime, metaClass);
  }

  private static class ConsumerAllocator implements ObjectAllocator {
    public IRubyObject allocate(Ruby runtime, RubyClass metaClass) {
      return new ConsumerWrapper(runtime, metaClass);
    }
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass producerClass = parentModule.defineClassUnder("Consumer", runtime.getObject(), new ConsumerAllocator());
    producerClass.defineAnnotatedMethods(ConsumerWrapper.class);
    return producerClass;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> convertKafkaOptions(ThreadContext ctx, IRubyObject config) {
    Map<String, Object> kafkaConfig = new HashMap<>();
    RubyHash configHash = config.convertToHash();
    for (IRubyObject key : (List<IRubyObject>) configHash.keys().getList()) {
      IRubyObject value = configHash.fastARef(key);
      if (key instanceof RubySymbol && !value.isNil()) {
        if (key.asJavaString().equals("bootstrap_servers")) {
          String valueString;
          if (value instanceof RubyArray) {
            valueString = value.convertToArray().join(ctx, ctx.runtime.newString(",")).asJavaString();
          } else {
            valueString = value.asString().asJavaString();
          }
          kafkaConfig.put("bootstrap.servers", valueString);
        }
      } else if (!value.isNil()) {
        kafkaConfig.put(key.asJavaString(), value.asString().asJavaString());
      }
    }
    return kafkaConfig;
  }

  @JRubyMethod(required = 1)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject config) {
    try {
      Deserializer<IRubyObject> deserializer = new RubyStringDeserializer(ctx.runtime);
      kafkaConsumer = new KafkaConsumer<>(convertKafkaOptions(ctx, config), deserializer, deserializer);
      return this;
    } catch (KafkaException ke) {
      throw KafkaClientsLibrary.newRaiseException(ctx.runtime, ke);
    }
  }

  @JRubyMethod
  public IRubyObject close(ThreadContext ctx) {
    kafkaConsumer.close();
    return ctx.runtime.getNil();
  }

  @SuppressWarnings("unchecked")
  @JRubyMethod(required = 1)
  public IRubyObject subscribe(ThreadContext ctx, IRubyObject topicNames) {
    Set<String> topics = new HashSet<>();
    RubyArray topicList = topicNames.convertToArray();
    for (IRubyObject topic : (List<IRubyObject>) topicList.getList()) {
      topics.add(topic.asString().asJavaString());
    }
    kafkaConsumer.subscribe(topics);
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject poll(ThreadContext ctx, IRubyObject timeout) {
    try {
      long timeoutMs = (long) timeout.convertToFloat().getDoubleValue() * 1000;
      ConsumerRecords<IRubyObject, IRubyObject> records = kafkaConsumer.poll(timeoutMs);
      return ConsumerRecordsWrapper.create(ctx.runtime, records);
    } catch (KafkaException ke) {
      throw KafkaClientsLibrary.newRaiseException(ctx.runtime, ke);
    }
  }
}
