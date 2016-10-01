package io.burt.kafka.clients;

import java.util.ArrayList;
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
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
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
import org.jruby.runtime.Block;
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
            topicPartitions.append(TopicPartitionWrapper.create(ctx.runtime, tp));
          }
          listener.callMethod(ctx, "on_partitions_revoked", topicPartitions);
        }
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (listener != null && listener.respondsTo("on_partitions_assigned")) {
          RubyArray topicPartitions = ctx.runtime.newArray(partitions.size());
          for (TopicPartition tp : partitions) {
            topicPartitions.append(TopicPartitionWrapper.create(ctx.runtime, tp));
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
  public IRubyObject subscription(ThreadContext ctx) {
    Set<String> topicNames = kafkaConsumer.subscription();
    RubyArray topics = ctx.runtime.newArray(topicNames.size());
    for (String topicName : topicNames) {
      topics.append(ctx.runtime.newString(topicName));
    }
    return topics;
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

  @SuppressWarnings("unchecked")
  private Map<TopicPartition, OffsetAndMetadata> toOffsets(ThreadContext ctx, IRubyObject arg) {
    Map<TopicPartition, OffsetAndMetadata> syncOffsets = new HashMap<>();
    RubyHash offsets = arg.convertToHash();
    for (IRubyObject key : (List<IRubyObject>) offsets.keys().getList()) {
      IRubyObject value = offsets.fastARef(key);
      if (key instanceof TopicPartitionWrapper) {
        if (value instanceof OffsetAndMetadataWrapper) {
          TopicPartition tp = ((TopicPartitionWrapper) key).topicPartition();
          OffsetAndMetadata om = ((OffsetAndMetadataWrapper) value).offsetAndMetadata();
          syncOffsets.put(tp, om);
        } else {
          throw ctx.runtime.newTypeError(value, ctx.runtime.getClassFromPath("Kafka::Clients::OffsetAndMetadata"));
        }
      } else {
        throw ctx.runtime.newTypeError(key, ctx.runtime.getClassFromPath("Kafka::Clients::TopicPartition"));
      }
    }
    return syncOffsets;
  }

  private RubyHash fromOffsets(ThreadContext ctx, Map<TopicPartition, OffsetAndMetadata> syncOffsets) {
    RubyHash offsets = RubyHash.newHash(ctx.runtime);
    for (TopicPartition tp : syncOffsets.keySet()) {
      OffsetAndMetadata om = syncOffsets.get(tp);
      offsets.fastASet(TopicPartitionWrapper.create(ctx.runtime, tp), OffsetAndMetadataWrapper.create(ctx.runtime, om));
    }
    return offsets;
  }

  @JRubyMethod(name = "commit_sync", optional = 1)
  public IRubyObject commitSync(ThreadContext ctx, IRubyObject[] args) {
    if (args.length == 0) {
      kafkaConsumer.commitSync();
    } else {
      kafkaConsumer.commitSync(toOffsets(ctx, args[0]));
    }
    return ctx.runtime.getNil();
  }

  @JRubyMethod(name = "commit_async", optional = 1)
  public IRubyObject commitAsync(final ThreadContext ctx, IRubyObject[] args, final Block block) {
    OffsetCommitCallback callback = new OffsetCommitCallback() {
      @Override
      public void onComplete(Map<TopicPartition, OffsetAndMetadata> syncOffsets, Exception exception) {
        if (block.isGiven()) {
          IRubyObject error = ctx.runtime.getNil();
          if (exception != null) {
            RubyClass errorClass = KafkaClientsLibrary.mapErrorClass(ctx.runtime, exception);
            error = errorClass.newInstance(ctx, ctx.runtime.newString(exception.getMessage()), Block.NULL_BLOCK);
          }
          block.call(ctx, new IRubyObject[] {fromOffsets(ctx, syncOffsets), error});
        }
      }
    };
    if (args.length == 0) {
      kafkaConsumer.commitAsync(callback);
    } else {
      kafkaConsumer.commitAsync(toOffsets(ctx, args[0]), callback);
    }
    return ctx.runtime.getNil();
  }

  @JRubyMethod(required = 1, optional = 1)
  public IRubyObject position(ThreadContext ctx, IRubyObject[] args) {
    TopicPartition tp = TopicPartitionWrapper.toTopicPartition(ctx, args);
    try {
      long offset = kafkaConsumer.position(tp);
      return ctx.runtime.newFixnum(offset);
    } catch (IllegalArgumentException iae) {
      throw ctx.runtime.newArgumentError(iae.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  private List<TopicPartition> toTopicPartitionList(ThreadContext ctx, IRubyObject partitions) {
    RubyArray tpa = partitions.convertToArray();
    List<TopicPartition> tpl = new ArrayList<>(tpa.size());
    for (IRubyObject tp : (List<IRubyObject>) tpa.getList()) {
      tpl.add(TopicPartitionWrapper.toTopicPartition(ctx, tp));
    }
    return tpl;
  }

  @SuppressWarnings("unchecked")
  @JRubyMethod(name = "seek_to_beginning", required = 1)
  public IRubyObject seekToBeginning(ThreadContext ctx, IRubyObject partitions) {
    kafkaConsumer.seekToBeginning(toTopicPartitionList(ctx, partitions));
    return ctx.runtime.getNil();
  }

  @SuppressWarnings("unchecked")
  @JRubyMethod(name = "seek_to_end", required = 1)
  public IRubyObject seekToEnd(ThreadContext ctx, IRubyObject partitions) {
    kafkaConsumer.seekToEnd(toTopicPartitionList(ctx, partitions));
    return ctx.runtime.getNil();
  }

  @JRubyMethod(required = 2, optional = 1)
  public IRubyObject seek(ThreadContext ctx, IRubyObject[] args) {
    TopicPartition tp;
    if (args.length == 3) {
      tp = TopicPartitionWrapper.toTopicPartition(ctx, args);
    } else if (args[0] instanceof TopicPartitionWrapper) {
      tp = ((TopicPartitionWrapper) args[0]).topicPartition();
    } else {
      throw ctx.runtime.newTypeError(args[0], ctx.runtime.getClassFromPath("Kafka::Clients::TopicPartition"));
    }
    kafkaConsumer.seek(tp, args[args.length - 1].convertToInteger().getLongValue());
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject assignment(ThreadContext ctx) {
    Set<TopicPartition> topicPartitions = kafkaConsumer.assignment();
    RubyArray array = ctx.runtime.newArray(topicPartitions.size());
    for (TopicPartition topicPartition : topicPartitions) {
      array.append(TopicPartitionWrapper.create(ctx.runtime, topicPartition));
    }
    return array;
  }

  @JRubyMethod(required = 1)
  public IRubyObject assign(ThreadContext ctx, IRubyObject partitions) {
    kafkaConsumer.assign(toTopicPartitionList(ctx, partitions));
    return ctx.runtime.getNil();
  }

  @JRubyMethod(name = "list_topics")
  public IRubyObject listTopics(ThreadContext ctx) {
    Map<String, List<PartitionInfo>> topicsAndPartitions = kafkaConsumer.listTopics();
    RubyHash topics = RubyHash.newHash(ctx.runtime);
    for (String t : topicsAndPartitions.keySet()) {
      List<PartitionInfo> pis = topicsAndPartitions.get(t);
      RubyArray partitionInfos = ctx.runtime.newArray(pis.size());
      for (PartitionInfo pi : pis) {
        partitionInfos.append(PartitionInfoWrapper.create(ctx.runtime, pi));
      }
      topics.fastASet(ctx.runtime.newString(t), partitionInfos);
    }
    return topics;
  }

  @JRubyMethod
  public IRubyObject paused(ThreadContext ctx) {
    Collection<TopicPartition> pausedPartitions = kafkaConsumer.paused();
    RubyArray paused = ctx.runtime.newArray(pausedPartitions.size());
    for (TopicPartition tp : pausedPartitions) {
      paused.append(TopicPartitionWrapper.create(ctx.runtime, tp));
    }
    return paused;
  }

  @SuppressWarnings("unchecked")
  private Set<TopicPartition> toTopicPartitions(ThreadContext ctx, IRubyObject arg) {
    Set<TopicPartition> topicPartitions = new HashSet<>();
    RubyArray partitions= arg.convertToArray();
    for (IRubyObject tp : (List<IRubyObject>) partitions.getList()) {
      if (tp instanceof TopicPartitionWrapper) {
        topicPartitions.add(((TopicPartitionWrapper) tp).topicPartition());
      } else {
        throw ctx.runtime.newTypeError(tp, "Kafka::Clients::TopicPartition");
      }
    }
    return topicPartitions;
  }

  @JRubyMethod
  public IRubyObject pause(ThreadContext ctx, IRubyObject partitions) {
    kafkaConsumer.pause(toTopicPartitions(ctx, partitions));
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject resume(ThreadContext ctx, IRubyObject partitions) {
    kafkaConsumer.resume(toTopicPartitions(ctx, partitions));
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject wakeup(ThreadContext ctx) {
    return ctx.runtime.getNil();
  }
}
