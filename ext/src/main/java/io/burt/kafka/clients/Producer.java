package io.burt.kafka.clients;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@SuppressWarnings("serial")
@JRubyClass(name = "Kafka::Clients::Producer")
public class Producer extends RubyObject {
  private KafkaProducer<IRubyObject, IRubyObject> kafkaProducer;

  public Producer(Ruby runtime, RubyClass metaClass) {
    super(runtime, metaClass);
  }

  private static class ProducerAllocator implements ObjectAllocator {
    public IRubyObject allocate(Ruby runtime, RubyClass metaClass) {
      return new Producer(runtime, metaClass);
    }
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass producerClass = parentModule.defineClassUnder("Producer", runtime.getObject(), new ProducerAllocator());
    producerClass.defineAnnotatedMethods(Producer.class);
    return producerClass;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> convertKafkaOptions(IRubyObject config) {
    Map<String, Object> kafkaConfig = new HashMap<>();
    RubyHash configHash = config.convertToHash();
    for (IRubyObject key : (List<IRubyObject>) configHash.keys().getList()) {
      IRubyObject value = configHash.fastARef(key);
      if (value != null && !value.isNil()) {
        kafkaConfig.put(key.asJavaString(), value.asString().asJavaString());
      }
    }
    return kafkaConfig;
  }

  @JRubyMethod(required = 1)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject config) {
    try {
      kafkaProducer = new KafkaProducer<IRubyObject, IRubyObject>(convertKafkaOptions(config), new RubyObjectSerializer(), new RubyObjectSerializer());
      return ctx.runtime.getNil();
    } catch (ConfigException ce) {
      throw KafkaClientsLibrary.newRaiseException(ctx.runtime, ce);
    }
  }

  @JRubyMethod(optional = 1)
  public IRubyObject close(ThreadContext ctx, IRubyObject[] args) {
    long timeout = -1;
    if (args.length > 0) {
      RubyHash options = args[0].convertToHash();
      IRubyObject timeoutOption = options.fastARef(ctx.runtime.newString("timeout"));
      if (timeoutOption!= null && !timeoutOption.isNil()) {
        timeout = (long) Math.floor(timeoutOption.convertToFloat().getDoubleValue() * 1000);
      }
    }
    if (timeout >= 0) {
      kafkaProducer.close(timeout, TimeUnit.MILLISECONDS);
    } else {
      kafkaProducer.close();
    }
    return ctx.runtime.getNil();
  }

  @JRubyMethod(required = 3)
  public IRubyObject send(final ThreadContext ctx, IRubyObject topic, IRubyObject key, IRubyObject value, final Block block) {
    String topicName = topic.asJavaString();
    ProducerRecord<IRubyObject, IRubyObject> record = new ProducerRecord<IRubyObject, IRubyObject>(topicName, key, value);
    Future<RecordMetadata> resultFuture;
    if (block.isGiven()) {
      resultFuture = kafkaProducer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata md, Exception exception) {
          IRubyObject error;
          if (exception == null) {
            error = ctx.runtime.getNil();
          } else {
            RubyClass errorClass = KafkaClientsLibrary.mapErrorClass(ctx.runtime, exception);
            error = errorClass.newInstance(ctx, ctx.runtime.newString(exception.getMessage()), Block.NULL_BLOCK);
          }
          IRubyObject metadata;
          if (md == null) {
            metadata = ctx.runtime.getNil();
          } else {
            metadata = RecordMetadataWrapper.create(ctx.runtime, md);
          }
          block.call(ctx, metadata, error);
        }
      });
    } else {
      resultFuture = kafkaProducer.send(record);
    }
    return FutureWrapper.create(ctx.runtime, resultFuture, new FutureWrapper.Rubifier<RecordMetadata>() {
      @Override
      public IRubyObject transform(RecordMetadata value) {
        return RecordMetadataWrapper.create(ctx.runtime, value);
      }
    });
  }

  @JRubyMethod
  public IRubyObject flush(ThreadContext ctx) {
    kafkaProducer.flush();
    return ctx.runtime.getNil();
  }

  @JRubyMethod(name = "partitions_for")
  public IRubyObject partitionsFor(ThreadContext ctx, IRubyObject topic) {
    RubyArray partitions = ctx.runtime.newArray();
    for (PartitionInfo partition : kafkaProducer.partitionsFor(topic.asJavaString())) {
      partitions.add(PartitionInfoWrapper.create(ctx.runtime, partition));
    }
    return partitions;
  }
}
