package io.burt.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@SuppressWarnings("serial")
@JRubyClass(name = "Kafka::Clients::PartitionInfo")
public class PartitionInfoWrapper extends RubyObject {
  private final PartitionInfo partition;

  private NodeWrapper leader;
  private RubyArray replicas;
  private RubyArray inSyncReplicas;

  public PartitionInfoWrapper(Ruby runtime, RubyClass metaClass, PartitionInfo partition) {
    super(runtime, metaClass);
    this.partition = partition;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass cls = parentModule.defineClassUnder("PartitionInfo", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    cls.defineAnnotatedMethods(PartitionInfoWrapper.class);
    return cls;
  }

  static PartitionInfoWrapper create(Ruby runtime, PartitionInfo partition) {
    return new PartitionInfoWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::PartitionInfo"), partition);
  }

  @JRubyMethod
  public IRubyObject topic(ThreadContext ctx) {
    return ctx.runtime.newString(partition.topic());
  }

  @JRubyMethod
  public IRubyObject partition(ThreadContext ctx) {
    return ctx.runtime.newFixnum(partition.partition());
  }

  @JRubyMethod
  public IRubyObject leader(ThreadContext ctx) {
    if (leader == null) {
      leader = NodeWrapper.create(ctx.runtime, partition.leader());
    }
    return leader;
  }

  @JRubyMethod
  public IRubyObject replicas(ThreadContext ctx) {
    if (replicas == null) {
      Node[] rs = partition.replicas();
      RubyArray rsw = ctx.runtime.newArray(rs.length);
      for (Node replica : rs) {
        rsw.add(NodeWrapper.create(ctx.runtime, replica));
      }
      replicas = rsw;
    }
    return replicas;
  }

  @JRubyMethod(name = "in_sync_replicas")
  public IRubyObject inSyncReplicas(ThreadContext ctx) {
    if (inSyncReplicas == null) {
      Node[] rs = partition.inSyncReplicas();
      RubyArray rsw = ctx.runtime.newArray(rs.length);
      for (Node replica : rs) {
        rsw.add(NodeWrapper.create(ctx.runtime, replica));
      }
      inSyncReplicas = rsw;
    }
    return inSyncReplicas;
  }

  @JRubyMethod
  public IRubyObject hash(ThreadContext ctx) {
    return ctx.runtime.newFixnum(partition.hashCode());
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject isEql(ThreadContext ctx, IRubyObject other) {
    if (other instanceof PartitionInfoWrapper) {
      return ctx.runtime.newBoolean(partition.equals(((PartitionInfoWrapper) other).partition));
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }
}
