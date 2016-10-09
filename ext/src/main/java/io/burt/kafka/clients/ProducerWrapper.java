package io.burt.kafka.clients;

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
import org.jruby.RubyProc;
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
    this(runtime, metaClass, null);
  }

  public ProducerWrapper(Ruby runtime, RubyClass metaClass, Producer<IRubyObject, IRubyObject> producer) {
    super(runtime, metaClass);
    this.kafkaProducer = producer;
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

  static ProducerWrapper create(Ruby runtime, Producer<IRubyObject, IRubyObject> producer) {
    JRubyClass annotation = ProducerWrapper.class.getAnnotation(JRubyClass.class);
    RubyClass metaClass = (RubyClass) runtime.getClassFromPath(annotation.name()[0]);
    return new ProducerWrapper(runtime, metaClass, producer);
  }

  @JRubyMethod(optional = 1)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject[] args) {
    if (kafkaProducer == null) {
      if (args.length > 0) {
        try {
          Serializer<IRubyObject> serializer = new RubyStringSerializer();
          Map<String, Object> config = KafkaClientsLibrary.toKafkaConfiguration(args[0].convertToHash());
          kafkaProducer = new KafkaProducer<>(config, serializer, serializer);
        } catch (KafkaException ke) {
          throw KafkaClientsLibrary.newRaiseException(ctx.runtime, ke);
        }
      } else {
        throw ctx.runtime.newArgumentError(0, 1);
      }
    }
    return this;
  }

  @JRubyMethod(optional = 1)
  public IRubyObject close(ThreadContext ctx, IRubyObject[] args) {
    long timeout = -1;
    if (args.length > 0) {
      RubyHash options = args[0].convertToHash();
      IRubyObject timeoutOption = options.fastARef(ctx.runtime.newSymbol("timeout"));
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
  public IRubyObject send(final ThreadContext ctx, IRubyObject[] args, Block block) {
    final RubyProc callback = block.isGiven() ? ctx.runtime.newProc(Block.Type.PROC, block) : null;
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
      if (callback != null) {
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
            callback.call(ctx, new IRubyObject[] {metadata, error});
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
    List<PartitionInfo> partitionInfos = kafkaProducer.partitionsFor(topic.asJavaString());
    if (partitionInfos != null) {
      for (PartitionInfo partition : partitionInfos) {
        partitions.add(PartitionInfoWrapper.create(ctx.runtime, partition));
      }
    }
    return partitions;
  }
}
