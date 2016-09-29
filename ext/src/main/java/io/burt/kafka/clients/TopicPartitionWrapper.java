package io.burt.kafka.clients;

import org.apache.kafka.common.TopicPartition;
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
@JRubyClass(name = "Kafka::Clients::TopicPartition")
public class TopicPartitionWrapper extends RubyObject {
  private TopicPartition topicPartition;
  private IRubyObject topic;
  private IRubyObject partition;

  public TopicPartitionWrapper(Ruby runtime, RubyClass metaClass) {
    this(runtime, metaClass, null);
  }

  public TopicPartitionWrapper(Ruby runtime, RubyClass metaClass, TopicPartition topicPartition) {
    super(runtime, metaClass);
    if (topicPartition != null) {
      this.topicPartition = topicPartition;
      this.topic = runtime.newString(topicPartition.topic());
      this.partition = runtime.newFixnum(topicPartition.partition());
    }
  }

  private static class TopicPartitionWrapperAllocator implements ObjectAllocator {
    public IRubyObject allocate(Ruby runtime, RubyClass metaClass) {
      return new TopicPartitionWrapper(runtime, metaClass);
    }
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass cls = parentModule.defineClassUnder("TopicPartition", runtime.getObject(), new TopicPartitionWrapperAllocator());
    cls.defineAnnotatedMethods(TopicPartitionWrapper.class);
    return cls;
  }

  static TopicPartitionWrapper create(Ruby runtime, TopicPartition topicPartition) {
    return new TopicPartitionWrapper(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::TopicPartition"), topicPartition);
  }

  static TopicPartition toTopicPartition(ThreadContext ctx, IRubyObject... args) {
    if (args.length > 1) {
      return new TopicPartition(args[0].convertToString().asJavaString(), (int) args[1].convertToInteger().getLongValue());
    } else if (args[0] instanceof TopicPartitionWrapper) {
      return ((TopicPartitionWrapper) args[0]).topicPartition();
    } else {
      throw ctx.runtime.newTypeError(args[0], ctx.runtime.getClassFromPath("Kafka::Clients::TopicPartition"));
    }
  }

  TopicPartition topicPartition() {
    return topicPartition;
  }

  @JRubyMethod(required = 2)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject topic, IRubyObject partition) {
    this.topic = topic;
    this.partition = partition;
    this.topicPartition = toTopicPartition(ctx, topic, partition);
    return this;
  }

  @JRubyMethod
  public IRubyObject topic(ThreadContext ctx) {
    return topic;
  }

  @JRubyMethod
  public IRubyObject partition(ThreadContext ctx) {
    return partition;
  }

  @JRubyMethod
  public IRubyObject hash(ThreadContext ctx) {
    return ctx.runtime.newFixnum(topicPartition.hashCode());
  }

  @JRubyMethod(name = "eql?", alias = {"=="})
  public IRubyObject isEql(ThreadContext ctx, IRubyObject other) {
    if (other instanceof TopicPartitionWrapper) {
      TopicPartition otherTopicPartition = ((TopicPartitionWrapper) other).topicPartition;
      if (this.topicPartition == null) {
        return ctx.runtime.newBoolean(otherTopicPartition == null);
      } else {
        return ctx.runtime.newBoolean(topicPartition.equals(otherTopicPartition));
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
      "#<%s @topic=\"%s\", @partition=%d>",
      getMetaClass().getName(),
      topicPartition.topic(),
      topicPartition.partition()
    ));
  }
}
