package io.burt.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@SuppressWarnings("serial")
@JRubyClass(name = "Kafka::Clients::ConsumerRecord")
public class ConsumerRecordWrapper extends RubyObject {
  private ConsumerRecord<IRubyObject, IRubyObject> record;
  private IRubyObject topic;
  private IRubyObject partition;
  private IRubyObject checksum;
  private IRubyObject timestamp;

  public ConsumerRecordWrapper(Ruby runtime, RubyClass metaClass) {
    this(runtime, metaClass, null);
  }

  public ConsumerRecordWrapper(Ruby runtime, RubyClass metaClass, ConsumerRecord<IRubyObject, IRubyObject> record) {
    super(runtime, metaClass);
    this.record = record;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass cls = parentModule.defineClassUnder("ConsumerRecord", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    cls.defineAnnotatedMethods(ConsumerRecordWrapper.class);
    return cls;
  }

  static ConsumerRecordWrapper create(Ruby runtime, ConsumerRecord<IRubyObject, IRubyObject> record) {
    return new ConsumerRecordWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::ConsumerRecord"), record);
  }

  ConsumerRecord<IRubyObject, IRubyObject> consumerRecord() {
    return record;
  }

  @JRubyMethod
  public IRubyObject topic(ThreadContext ctx) {
    if (topic == null) {
      topic = ctx.runtime.newString(record.topic());
    }
    return topic;
  }

  @JRubyMethod
  public IRubyObject partition(ThreadContext ctx) {
    if (partition == null) {
      partition = ctx.runtime.newFixnum(record.partition());
    }
    return partition;
  }

  @JRubyMethod
  public IRubyObject checksum(ThreadContext ctx) {
    if (checksum == null) {
      checksum = ctx.runtime.newFixnum(record.checksum());
    }
    return checksum;
  }

  @JRubyMethod
  public IRubyObject key(ThreadContext ctx) {
    return record.key();
  }

  @JRubyMethod
  public IRubyObject value(ThreadContext ctx) {
    return record.value();
  }

  @JRubyMethod
  public IRubyObject timestamp(ThreadContext ctx) {
    if (timestamp == null) {
      timestamp = ctx.runtime.newTime(record.timestamp());
    }
    return timestamp;
  }

  @JRubyMethod
  public IRubyObject hash(ThreadContext ctx) {
    return ctx.runtime.newFixnum(record.hashCode());
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject isEql(ThreadContext ctx, IRubyObject other) {
    if (other instanceof ConsumerRecordWrapper) {
      return ctx.runtime.newBoolean(record.equals(((ConsumerRecordWrapper) other).record));
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }
}
