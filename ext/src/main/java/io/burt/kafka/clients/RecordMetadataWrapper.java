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
    RubyClass cls = parentModule.defineClassUnder("RecordMetadata", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    cls.defineAnnotatedMethods(RecordMetadataWrapper.class);
    return cls;
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

  @Override
  public String toString() {
    return toS(Ruby.getGlobalRuntime().getCurrentContext()).toString();
  }

  @JRubyMethod(name = "to_s")
  public IRubyObject toS(ThreadContext ctx) {
    return ctx.runtime.newString(String.format(
      "#<%s @topic=\"%s\", @partition=%d, @offset=%d, @timestamp=%s, @checksum=%d, @serialized_key_size=%d, @serialized_value_size=%d>",
      getMetaClass().getName(),
      metadata.topic(),
      metadata.partition(),
      metadata.offset(),
      timestamp(ctx),
      metadata.checksum(),
      metadata.serializedKeySize(),
      metadata.serializedValueSize()
    ));
  }
}
