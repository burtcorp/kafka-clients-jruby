package io.burt.kafka.clients;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.jruby.Ruby;
import org.jruby.runtime.builtin.IRubyObject;

public class PartitionerProxy implements Partitioner {
  private IRubyObject partitioner;

  @Override
  public void configure(Map<String, ?> config) {
    partitioner = (IRubyObject) config.get("io.burt.kafka.clients.partitioner");
  }

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Ruby runtime = partitioner.getRuntime();
    ClusterWrapper clusterWrapper = ClusterWrapper.create(runtime, cluster);
    IRubyObject[] args = new IRubyObject[] {runtime.newString(topic), (IRubyObject) key, (IRubyObject) value, clusterWrapper};
    IRubyObject partition = partitioner.callMethod(runtime.getCurrentContext(), "partition", args);
    return (int) partition.convertToInteger().getLongValue();
  }

  @Override
  public void close() {
    if (partitioner.respondsTo("close")) {
      partitioner.callMethod(partitioner.getRuntime().getCurrentContext(), "close");
    }
  }
}
