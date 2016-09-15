package io.burt.kafka.clients;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@SuppressWarnings("serial")
@JRubyClass(name = "Kafka::Clients::Producer")
public class ProducerWrapper extends RubyObject {
  private Producer<IRubyObject, IRubyObject> kafkaProducer;

  public ProducerWrapper(Ruby runtime, RubyClass metaClass) {
    super(runtime, metaClass);
  }

  private static class ProducerAllocator implements ObjectAllocator {
    public IRubyObject allocate(Ruby runtime, RubyClass metaClass) {
      return new ProducerWrapper(runtime, metaClass);
    }
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass cls = parentModule.defineClassUnder("Producer", runtime.getObject(), new ProducerAllocator());
    cls.defineAnnotatedMethods(ProducerWrapper.class);
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
        } else if (key.asJavaString().equals("partitioner")) {
          kafkaConfig.put("partitioner.class", "io.burt.kafka.clients.PartitionerProxy");
          kafkaConfig.put("io.burt.kafka.clients.partitioner", value);
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
      Serializer<IRubyObject> serializer = new RubyStringSerializer();
      kafkaProducer = new KafkaProducer<IRubyObject, IRubyObject>(convertKafkaOptions(ctx, config), serializer, serializer);
      return this;
    } catch (KafkaException ke) {
      throw KafkaClientsLibrary.newRaiseException(ctx.runtime, ke);
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

  @JRubyMethod(required = 1, optional = 4)
  public IRubyObject send(final ThreadContext ctx, IRubyObject[] args, final Block block) {
    ProducerRecord<IRubyObject, IRubyObject> record;
    if (args.length == 1) {
      if (args[0] instanceof ProducerRecordWrapper) {
        record = ((ProducerRecordWrapper) args[0]).producerRecord();
      } else {
        throw ctx.runtime.newTypeError(args[0], ctx.runtime.getClassFromPath("Kafka::Clients::ProducerRecord"));
      }
    } else {
      record = ProducerRecordWrapper.toProducerRecord(args);
    }
    Future<RecordMetadata> resultFuture;
    try {
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
    } catch (IllegalArgumentException iae) {
      throw ctx.runtime.newArgumentError(iae.getMessage());
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
