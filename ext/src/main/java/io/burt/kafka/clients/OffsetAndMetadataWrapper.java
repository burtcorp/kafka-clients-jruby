package io.burt.kafka.clients;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

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
@JRubyClass(name = "Kafka::Clients::OffsetAndMetadata")
public class OffsetAndMetadataWrapper extends RubyObject {
  private OffsetAndMetadata offsetAndMetadata;
  private IRubyObject offset;
  private IRubyObject metadata;

  public OffsetAndMetadataWrapper(Ruby runtime, RubyClass metaClass) {
    this(runtime, metaClass, null);
  }

  public OffsetAndMetadataWrapper(Ruby runtime, RubyClass metaClass, OffsetAndMetadata offsetAndMetadata) {
    super(runtime, metaClass);
    if (offsetAndMetadata != null) {
      this.offsetAndMetadata = offsetAndMetadata;
      this.offset = runtime.newFixnum(offsetAndMetadata.offset());
      this.metadata = offsetAndMetadata.metadata() == null ? runtime.getNil() : runtime.newString(offsetAndMetadata.metadata());
    }
  }

  private static class offsetAndMetadataWrapperAllocator implements ObjectAllocator {
    public IRubyObject allocate(Ruby runtime, RubyClass metaClass) {
      return new OffsetAndMetadataWrapper(runtime, metaClass);
    }
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass cls = parentModule.defineClassUnder("OffsetAndMetadata", runtime.getObject(), new offsetAndMetadataWrapperAllocator());
    cls.defineAnnotatedMethods(OffsetAndMetadataWrapper.class);
    return cls;
  }

  static OffsetAndMetadataWrapper create(Ruby runtime, OffsetAndMetadata offsetAndMetadata) {
    return new OffsetAndMetadataWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::OffsetAndMetadata"), offsetAndMetadata);
  }

  OffsetAndMetadata offsetAndMetadata() {
    return offsetAndMetadata;
  }

  @JRubyMethod(required = 1, optional = 1)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject[] args) {
    this.offset = args[0];
    this.metadata = args.length > 1 ? args[1] : ctx.runtime.getNil();
    this.offsetAndMetadata = new OffsetAndMetadata(
      offset.convertToInteger().getLongValue(),
      this.metadata.isNil() ? null : this.metadata.convertToString().asJavaString()
    );
    return this;
  }

  @JRubyMethod
  public IRubyObject offset() {
    return offset;
  }

  @JRubyMethod
  public IRubyObject metadata() {
    return metadata;
  }

  @JRubyMethod
  public IRubyObject hash(ThreadContext ctx) {
    return ctx.runtime.newFixnum(offsetAndMetadata.hashCode());
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject isEql(ThreadContext ctx, IRubyObject other) {
    if (other instanceof OffsetAndMetadataWrapper) {
      OffsetAndMetadata otherOffsetAndMetadata = ((OffsetAndMetadataWrapper) other).offsetAndMetadata;
      if (this.offsetAndMetadata == null) {
        return ctx.runtime.newBoolean(otherOffsetAndMetadata== null);
      } else {
        return ctx.runtime.newBoolean(offsetAndMetadata.equals(otherOffsetAndMetadata));
      }
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }

  @Override
  public String toString() {
    return toS(Ruby.getGlobalRuntime().getCurrentContext()).toString();
  }

  @JRubyMethod(name = "to_s")
  public IRubyObject toS(ThreadContext ctx) {
    return ctx.runtime.newString(String.format(
      "#<%s @offset=%d, @metadata=%s>",
      getMetaClass().getName(),
      offsetAndMetadata.offset(),
      metadata.inspect()
    ));
  }
}
