package io.burt.kafka.clients;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyString;
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
    RubyClass cls = parentModule.defineClassUnder("Consumer", runtime.getObject(), new ConsumerAllocator());
    cls.defineAnnotatedMethods(ConsumerWrapper.class);
    return cls;
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

  private ConsumerRebalanceListener createListener(final ThreadContext ctx, final IRubyObject listener) {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (listener != null && listener.respondsTo("on_partitions_revoked")) {
          RubyArray topicPartitions = ctx.runtime.newArray(partitions.size());
          for (TopicPartition tp : partitions) {
            topicPartitions.add(TopicPartitionWrapper.create(ctx.runtime, tp));
          }
          listener.callMethod(ctx, "on_partitions_revoked", topicPartitions);
        }
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (listener != null && listener.respondsTo("on_partitions_assigned")) {
          RubyArray topicPartitions = ctx.runtime.newArray(partitions.size());
          for (TopicPartition tp : partitions) {
            topicPartitions.add(TopicPartitionWrapper.create(ctx.runtime, tp));
          }
          listener.callMethod(ctx, "on_partitions_assigned", topicPartitions);
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  @JRubyMethod(required = 1, optional = 1)
  public IRubyObject subscribe(final ThreadContext ctx, IRubyObject[] args) {
    final IRubyObject topicNames = args[0];
    final IRubyObject listener = args.length > 1 ? args[1] : null;
    ConsumerRebalanceListener rebalanceListener = createListener(ctx, listener);
    if (topicNames.respondsTo("to_a")) {
      RubyArray topicNamesArray = topicNames.callMethod(ctx, "to_a").convertToArray();
      Set<String> topics = new HashSet<>();
      for (IRubyObject topic : (List<IRubyObject>) topicNamesArray.getList()) {
        topics.add(topic.asString().asJavaString());
      }
      kafkaConsumer.subscribe(topics, rebalanceListener);
    } else if (topicNames instanceof RubyString) { // TODO can this be done by not checking the Java class?
      Pattern topicPattern = Pattern.compile(topicNames.asJavaString());
      kafkaConsumer.subscribe(topicPattern, rebalanceListener);
    } else {
      throw ctx.runtime.newTypeError(topicNames, "Enumerable of topics or topic pattern");
    }
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject unsubscribe(ThreadContext ctx) {
    kafkaConsumer.unsubscribe();
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

  @JRubyMethod(name = "commit_sync", optional = 1)
  public IRubyObject commitSync(ThreadContext ctx, IRubyObject[] args) {
    if (args.length == 0) {
      kafkaConsumer.commitSync();
    } else {
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      kafkaConsumer.commitSync(offsets);
    }
    return ctx.runtime.getNil();
  }
}
