package io.burt.kafka.clients;

import java.util.List;
import java.util.Set;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
@JRubyClass(name = "Kafka::Clients::Cluster")
public class ClusterWrapper extends RubyObject {
  private final Cluster cluster;
  private final IRubyObject bootstrapConfigured;

  private RubyArray nodes;
  private RubyArray topics;
  private RubyArray unauthorizedTopics;

  public ClusterWrapper(Ruby runtime, RubyClass metaClass, Cluster cluster) {
    super(runtime, metaClass);
    this.cluster = cluster;
    this.bootstrapConfigured = runtime.newBoolean(cluster.isBootstrapConfigured());
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass futureClass = parentModule.defineClassUnder("Cluster", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    futureClass.defineAnnotatedMethods(ClusterWrapper.class);
    return futureClass;
  }

  static ClusterWrapper create(Ruby runtime, Cluster cluster) {
    return new ClusterWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::Cluster"), cluster);
  }

  @JRubyMethod
  public IRubyObject nodes(ThreadContext ctx) {
    if (nodes == null) {
      List<Node> cns = cluster.nodes();
      RubyArray ns = ctx.runtime.newArray(cns.size());
      for (Node n : cns) {
        ns.add(NodeWrapper.create(ctx.runtime, n));
      }
      nodes = ns;
    }
    return nodes;
  }

  @JRubyMethod(name = "node_by_id", required = 1)
  public IRubyObject nodeById(ThreadContext ctx, IRubyObject id) {
    int nodeId = (int) id.convertToInteger().getLongValue();
    return NodeWrapper.create(ctx.runtime, cluster.nodeById(nodeId));
  }

  @JRubyMethod(name = "leader_for", required = 2)
  public IRubyObject leaderFor(ThreadContext ctx, IRubyObject topic, IRubyObject partition) {
    String t = topic.convertToString().asJavaString();
    int p = (int) partition.convertToInteger().getLongValue();
    Node leader = cluster.leaderFor(new TopicPartition(t, p));
    return NodeWrapper.create(ctx.runtime, leader);
  }

  @JRubyMethod(required = 2)
  public IRubyObject partition(ThreadContext ctx, IRubyObject topic, IRubyObject partition) {
    String t = topic.convertToString().asJavaString();
    int p = (int) partition.convertToInteger().getLongValue();
    PartitionInfo partitionInfo = cluster.partition(new TopicPartition(t, p));
    return PartitionInfoWrapper.create(ctx.runtime, partitionInfo);
  }

  @JRubyMethod(name = "partitions_for_topic", required = 1)
  public IRubyObject partitionsForTopic(ThreadContext ctx, IRubyObject topic) {
    String t = topic.convertToString().asJavaString();
    List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(t);
    RubyArray partitionInfoWrappers = ctx.runtime.newArray(partitionInfos.size());
    for (PartitionInfo pi : partitionInfos) {
      partitionInfoWrappers.add(PartitionInfoWrapper.create(ctx.runtime, pi));
    }
    return partitionInfoWrappers;
  }

  @JRubyMethod(name = "available_partitions_for_topic", required = 1)
  public IRubyObject availablePartitionsForTopic(ThreadContext ctx, IRubyObject topic) {
    String t = topic.convertToString().asJavaString();
    List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(t);
    RubyArray partitionInfoWrappers = ctx.runtime.newArray(partitionInfos.size());
    for (PartitionInfo pi : partitionInfos) {
      partitionInfoWrappers.add(PartitionInfoWrapper.create(ctx.runtime, pi));
    }
    return partitionInfoWrappers;
  }

  @JRubyMethod(name = "partitions_for_node", required = 1)
  public IRubyObject partitionsForNode(ThreadContext ctx, IRubyObject nodeId) {
    int n = (int) nodeId.convertToInteger().getLongValue();
    List<PartitionInfo> partitionInfos = cluster.partitionsForNode(n);
    RubyArray partitionInfoWrappers = ctx.runtime.newArray(partitionInfos.size());
    for (PartitionInfo pi : partitionInfos) {
      partitionInfoWrappers.add(PartitionInfoWrapper.create(ctx.runtime, pi));
    }
    return partitionInfoWrappers;
  }

  @JRubyMethod(name = "partition_count_for_topic", required = 1)
  public IRubyObject partitionCountForTopic(ThreadContext ctx, IRubyObject topic) {
    String t = topic.convertToString().asJavaString();
    Integer count = cluster.partitionCountForTopic(t);
    if (count == null) {
      return ctx.runtime.getNil();
    } else {
      return ctx.runtime.newFixnum(count);
    }
  }

  @JRubyMethod
  public IRubyObject topics(ThreadContext ctx) {
    if (topics == null) {
      Set<String> topicNames = cluster.topics();
      RubyArray ts = ctx.runtime.newArray(topicNames.size());
      for (String topicName : topicNames) {
        ts.add(ctx.runtime.newString(topicName));
      }
      topics = ts;
    }
    return topics;
  }

  @JRubyMethod(name = "unauthorized_topics")
  public IRubyObject unauthorizedTopics(ThreadContext ctx) {
    if (unauthorizedTopics == null) {
      Set<String> topicNames = cluster.unauthorizedTopics();
      RubyArray ts = ctx.runtime.newArray(topicNames.size());
      for (String topicName : topicNames) {
        ts.add(ctx.runtime.newString(topicName));
      }
      unauthorizedTopics = ts;
    }
    return unauthorizedTopics;
  }

  @JRubyMethod(name = "bootstrap_configured?")
  public IRubyObject isBootstrapConfigured(ThreadContext ctx) {
    return bootstrapConfigured;
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject eql_p(ThreadContext ctx, IRubyObject other) {
    if (other instanceof ClusterWrapper) {
      return ctx.runtime.newBoolean(cluster.equals(((ClusterWrapper) other).cluster));
    } else {
      throw ctx.runtime.newTypeError(other, metaClass);
    }
  }
}
