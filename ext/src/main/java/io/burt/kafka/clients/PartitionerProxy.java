package io.burt.kafka.clients;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.jruby.Ruby;
import org.jruby.runtime.builtin.IRubyObject;

public class PartitionerProxy implements Partitioner {
  private Ruby runtime;
  private IRubyObject partitioner;
  private String partitionMethodName;

  @Override
  public void configure(Map<String, ?> config) {
    runtime = (Ruby) config.get(KafkaClientsLibrary.RUNTIME_CONFIG);
    partitioner = (IRubyObject) config.get(KafkaClientsLibrary.PARTITIONER_CONFIG);
    if (partitioner.respondsTo("partition")) {
      partitionMethodName = "partition";
    } else if (partitioner.respondsTo("call")) {
      partitionMethodName = "call";
    } else {
      throw runtime.newTypeError(partitioner, "object that responds to #partition or #call");
    }
  }

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    ClusterWrapper clusterWrapper = ClusterWrapper.create(runtime, cluster);
    IRubyObject[] args = new IRubyObject[] {runtime.newString(topic), (IRubyObject) key, (IRubyObject) value, clusterWrapper};
    IRubyObject partition = partitioner.callMethod(runtime.getCurrentContext(), partitionMethodName, args);
    return (int) partition.convertToInteger().getLongValue();
  }

  @Override
  public void close() {
    if (partitioner.respondsTo("close")) {
      partitioner.callMethod(partitioner.getRuntime().getCurrentContext(), "close");
    }
  }
}
