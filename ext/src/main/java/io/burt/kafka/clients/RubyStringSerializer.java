package io.burt.kafka.clients;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.jruby.Ruby;
import org.jruby.runtime.builtin.IRubyObject;

public class RubyStringSerializer implements Serializer<IRubyObject> {
  protected Ruby runtime;

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    runtime = (Ruby) config.get("io.burt.kafka.clients.runtime");
  }

  @Override
  public byte[] serialize(String topic, IRubyObject data) {
    if (data == null) {
      return null;
    } else if (data.isNil()) {
      return null;
    } else {
      return data.asString().getBytes();
    }
  }

  @Override
  public void close() { }
}
