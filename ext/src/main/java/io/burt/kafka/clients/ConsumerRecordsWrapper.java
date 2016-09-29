package io.burt.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@SuppressWarnings("serial")
@JRubyClass(name = "Kafka::Clients::ConsumerRecords")
public class ConsumerRecordsWrapper extends RubyObject {
  private final ConsumerRecords<IRubyObject, IRubyObject> records;

  public ConsumerRecordsWrapper(Ruby runtime, RubyClass metaClass, ConsumerRecords<IRubyObject, IRubyObject> records) {
    super(runtime, metaClass);
    this.records = records;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass cls = parentModule.defineClassUnder("ConsumerRecords", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    cls.defineAnnotatedMethods(ConsumerRecordsWrapper.class);
    cls.includeModule(runtime.getEnumerable());
    return cls;
  }

  static ConsumerRecordsWrapper create(Ruby runtime, ConsumerRecords<IRubyObject, IRubyObject> records) {
    return new ConsumerRecordsWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::ConsumerRecords"), records);
  }

  @JRubyMethod
  public IRubyObject count(ThreadContext ctx) {
    return ctx.runtime.newFixnum(records.count());
  }

  @JRubyMethod
  public IRubyObject each(ThreadContext ctx, Block block) {
    if (block.isGiven()) {
      for (ConsumerRecord<IRubyObject, IRubyObject> record : records) {
        block.call(ctx, new IRubyObject[] {ConsumerRecordWrapper.create(ctx.runtime, record)});
      }
    }
    return this;
  }

  @JRubyMethod(name = "empty?")
  public IRubyObject isEmpty(ThreadContext ctx) {
    return ctx.runtime.newBoolean(records.count() == 0);
  }

  @JRubyMethod
  public IRubyObject hash(ThreadContext ctx) {
    return ctx.runtime.newFixnum(records.hashCode());
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject isEql(ThreadContext ctx, IRubyObject other) {
    if (other instanceof ConsumerRecordsWrapper) {
      return ctx.runtime.newBoolean(records.equals(((ConsumerRecordsWrapper) other).records));
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }
}
