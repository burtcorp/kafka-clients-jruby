package io.burt.kafka.clients;

import org.apache.kafka.clients.producer.ProducerRecord;
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
@JRubyClass(name = "Kafka::Clients::ProducerRecord")
public class ProducerRecordWrapper extends RubyObject {
  private ProducerRecord<IRubyObject, IRubyObject> record;

  public ProducerRecordWrapper(Ruby runtime, RubyClass metaClass) {
    this(runtime, metaClass, null);
  }

  public ProducerRecordWrapper(Ruby runtime, RubyClass metaClass, ProducerRecord<IRubyObject, IRubyObject> record) {
    super(runtime, metaClass);
    this.record = record;
  }

  private static class ProducerRecordWrapperAllocator implements ObjectAllocator {
    public IRubyObject allocate(Ruby runtime, RubyClass metaClass) {
      return new ProducerRecordWrapper(runtime, metaClass);
    }
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass cls = parentModule.defineClassUnder("ProducerRecord", runtime.getObject(), new ProducerRecordWrapperAllocator());
    cls.defineAnnotatedMethods(ProducerRecordWrapper.class);
    return cls;
  }

  static ProducerRecordWrapper create(Ruby runtime, ProducerRecord<IRubyObject, IRubyObject> record) {
    return new ProducerRecordWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::ProducerRecord"), record);
  }

  ProducerRecord<IRubyObject, IRubyObject> producerRecord() {
    return record;
  }

  static ProducerRecord<IRubyObject, IRubyObject> toProducerRecord(IRubyObject[] args) {
    Ruby runtime = args[0].getRuntime();
    IRubyObject topic;
    IRubyObject partition = runtime.getNil();
    IRubyObject timestamp = runtime.getNil();
    IRubyObject key = runtime.getNil();
    IRubyObject value;
    if (args.length == 2) {
      topic = args[0];
      value = args[1];
    } else if (args.length == 3) {
      topic = args[0];
      key = args[1];
      value = args[2];
    } else if (args.length == 4) {
      topic = args[0].convertToString();
      partition = args[1].convertToInteger();
      key = args[2];
      value = args[3];
    } else {
      topic = args[0].convertToString();
      partition = args[1].convertToInteger();
      timestamp = args[2];
      key = args[3];
      value = args[4];
    }
    Integer p = null;
    if (!partition.isNil()) {
      p = (int) partition.convertToInteger().getLongValue();
    }
    Long ts = null;
    if (!timestamp.isNil()) {
      ts = (long) (timestamp.convertToFloat().getDoubleValue() * 1000);
    }
    return new ProducerRecord<>(topic.asJavaString(), p, ts, key, value);
  }

  @JRubyMethod(required = 2, optional = 3)
  public IRubyObject initialize(IRubyObject[] args) {
    this.record = toProducerRecord(args);
    return this;
  }

  @JRubyMethod
  public IRubyObject topic(ThreadContext ctx) {
    return ctx.runtime.newString(record.topic());
  }

  @JRubyMethod
  public IRubyObject partition(ThreadContext ctx) {
    Integer p = record.partition();
    if (p == null) {
      return ctx.runtime.getNil();
    } else {
      return ctx.runtime.newFixnum(p);
    }
  }

  @JRubyMethod
  public IRubyObject key() {
    return record.key();
  }

  @JRubyMethod
  public IRubyObject value() {
    return record.value();
  }

  @JRubyMethod
  public IRubyObject timestamp(ThreadContext ctx) {
    Long t = record.timestamp();
    if (t == null) {
      return ctx.runtime.getNil();
    } else {
      return ctx.runtime.newTime(t);
    }
  }

  @JRubyMethod
  public IRubyObject hash(ThreadContext ctx) {
    return ctx.runtime.newFixnum(record.hashCode());
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject isEql(ThreadContext ctx, IRubyObject other) {
    if (other instanceof ProducerRecordWrapper) {
      return ctx.runtime.newBoolean(record.equals(((ProducerRecordWrapper) other).record));
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }
}
