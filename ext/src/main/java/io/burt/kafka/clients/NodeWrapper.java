package io.burt.kafka.clients;

import org.apache.kafka.common.Node;

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
@JRubyClass(name = "Kafka::Clients::NodeWrapper")
public class NodeWrapper extends RubyObject {
  private final Node node;

  public NodeWrapper(Ruby runtime, RubyClass metaClass, Node node) {
    super(runtime, metaClass);
    this.node = node;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass futureClass = parentModule.defineClassUnder("NodeWrapper", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    futureClass.defineAnnotatedMethods(NodeWrapper.class);
    return futureClass;
  }

  static NodeWrapper create(Ruby runtime, Node node) {
    return new NodeWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::NodeWrapper"), node);
  }

  @JRubyMethod(name = "has_rack?")
  public IRubyObject has_rack_p(ThreadContext ctx) {
    return ctx.runtime.newBoolean(node.hasRack());
  }

  @JRubyMethod
  public IRubyObject rack(ThreadContext ctx) {
    if (node.hasRack()) {
      return ctx.runtime.newString(node.rack());
    } else {
      return ctx.runtime.getNil();
    }
  }

  @JRubyMethod
  public IRubyObject host(ThreadContext ctx) {
    return ctx.runtime.newString(node.host());
  }

  @JRubyMethod
  public IRubyObject id(ThreadContext ctx) {
    return ctx.runtime.newFixnum(node.id());
  }

  @JRubyMethod(name = "empty?")
  public IRubyObject empt_p(ThreadContext ctx) {
    return ctx.runtime.newBoolean(node.isEmpty());
  }

  @JRubyMethod
  public IRubyObject port(ThreadContext ctx) {
    return ctx.runtime.newFixnum(node.port());
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject eql_p(ThreadContext ctx, IRubyObject other) {
    if (other instanceof NodeWrapper) {
      return ctx.runtime.newBoolean(node.equals(((NodeWrapper) other).node));
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }
}
