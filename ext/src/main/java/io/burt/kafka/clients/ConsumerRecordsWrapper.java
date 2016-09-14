package io.burt.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecords;

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
@JRubyClass(name = "Kafka::Clients::ConsumerRecords")
public class ConsumerRecordsWrapper extends RubyObject {
  private final ConsumerRecords<IRubyObject, IRubyObject> records;

  public ConsumerRecordsWrapper(Ruby runtime, RubyClass metaClass, ConsumerRecords<IRubyObject, IRubyObject> records) {
    super(runtime, metaClass);
    this.records = records;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass futureClass = parentModule.defineClassUnder("ConsumerRecords", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    futureClass.defineAnnotatedMethods(ConsumerRecordsWrapper.class);
    return futureClass;
  }

  static ConsumerRecordsWrapper create(Ruby runtime, ConsumerRecords<IRubyObject, IRubyObject> records) {
    return new ConsumerRecordsWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::ConsumerRecords"), records);
  }

  @JRubyMethod
  public IRubyObject hash(ThreadContext ctx) {
    return ctx.runtime.newFixnum(records.hashCode());
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject eql_p(ThreadContext ctx, IRubyObject other) {
    if (other instanceof ConsumerRecordsWrapper) {
      return ctx.runtime.newBoolean(records.equals(((ConsumerRecordsWrapper) other).records));
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }
}
