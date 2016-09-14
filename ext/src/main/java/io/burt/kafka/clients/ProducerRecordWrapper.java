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
  private IRubyObject topic;
  private IRubyObject partition;
  private IRubyObject key;
  private IRubyObject value;
  private IRubyObject timestamp;

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
    RubyClass futureClass = parentModule.defineClassUnder("ProducerRecord", runtime.getObject(), new ProducerRecordWrapperAllocator());
    futureClass.defineAnnotatedMethods(ProducerRecordWrapper.class);
    return futureClass;
  }

  static ProducerRecordWrapper create(Ruby runtime, ProducerRecord<IRubyObject, IRubyObject> record) {
    return new ProducerRecordWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::ProducerRecord"), record);
  }

  ProducerRecord<IRubyObject, IRubyObject> producerRecord() {
    return record;
  }

  @JRubyMethod(required = 2, optional = 3)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject[] args) {
    this.key = ctx.runtime.getNil();
    this.partition = ctx.runtime.getNil();
    this.timestamp = ctx.runtime.getNil();
    if (args.length == 2) {
      this.topic = args[0];
      this.value = args[1];
    } else if (args.length == 3) {
      this.topic = args[0];
      this.key = args[1];
      this.value = args[2];
    } else if (args.length == 4) {
      this.topic = args[0].convertToString();
      this.partition = args[1].convertToInteger();
      this.key = args[2];
      this.value = args[3];
    } else {
      this.topic = args[0].convertToString();
      this.partition = args[1].convertToInteger();
      this.timestamp = args[2];
      this.key = args[3];
      this.value = args[4];
    }
    Integer p = null;
    if (!partition.isNil()) {
      p = (int) partition.convertToInteger().getLongValue();
    }
    Long ts = null;
    if (!timestamp.isNil()) {
      ts = (long) (timestamp.convertToFloat().getDoubleValue() * 1000);
    }
    this.record = new ProducerRecord<IRubyObject, IRubyObject>(topic.asJavaString(), p, ts, key, value);
    return this;
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject eql_p(ThreadContext ctx, IRubyObject other) {
    if (other instanceof ProducerRecordWrapper) {
      return ctx.runtime.newBoolean(record.equals(((ProducerRecordWrapper) other).record));
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }
}
