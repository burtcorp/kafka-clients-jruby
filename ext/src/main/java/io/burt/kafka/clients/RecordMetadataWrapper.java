package io.burt.kafka.clients;

import org.apache.kafka.clients.producer.RecordMetadata;

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
@JRubyClass(name = "Kafka::Clients::RecordMetadata")
public class RecordMetadataWrapper extends RubyObject {
  private final RecordMetadata metadata;

  public RecordMetadataWrapper(Ruby runtime, RubyClass metaClass, RecordMetadata metadata) {
    super(runtime, metaClass);
    this.metadata = metadata;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass futureClass = parentModule.defineClassUnder("RecordMetadata", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    futureClass.defineAnnotatedMethods(RecordMetadataWrapper.class);
    return futureClass;
  }

  static RecordMetadataWrapper create(Ruby runtime, RecordMetadata metadata) {
    return new RecordMetadataWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::RecordMetadata"), metadata);
  }

  @JRubyMethod
  public IRubyObject offset(ThreadContext ctx) {
    return ctx.runtime.newFixnum(metadata.offset());
  }

  @JRubyMethod
  public IRubyObject partition(ThreadContext ctx) {
    return ctx.runtime.newFixnum(metadata.partition());
  }

  @JRubyMethod
  public IRubyObject timestamp(ThreadContext ctx) {
    return ctx.runtime.newTime(metadata.timestamp());
  }

  @JRubyMethod
  public IRubyObject topic(ThreadContext ctx) {
    return ctx.runtime.newString(metadata.topic());
  }

  @JRubyMethod
  public IRubyObject checksum(ThreadContext ctx) {
    return ctx.runtime.newFixnum(metadata.checksum());
  }

  @JRubyMethod(name = "serialized_key_size")
  public IRubyObject serializedKeySize(ThreadContext ctx) {
    return ctx.runtime.newFixnum(metadata.serializedKeySize());
  }

  @JRubyMethod(name = "serialized_value_size")
  public IRubyObject serializedValueSize(ThreadContext ctx) {
    return ctx.runtime.newFixnum(metadata.serializedValueSize());
  }
}
