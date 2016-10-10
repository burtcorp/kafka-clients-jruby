package io.burt.kafka.clients;

import java.util.Map;

import org.jruby.runtime.builtin.IRubyObject;

public class DeserializerProxy extends RubyStringDeserializer {
  private IRubyObject deserializer;

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    super.configure(config, isKey);
    deserializer = (IRubyObject) config.get(String.format("io.burt.kafka.clients.deserializer.%s", isKey ? "key" : "value"));
  }

  @Override
  public IRubyObject deserialize(String topic, byte[] data) {
    if (data == null) {
      return super.deserialize(topic, data);
    } else {
      return deserializer.callMethod(runtime.getCurrentContext(), "deserialize", super.deserialize(topic, data));
    }
  }

  @Override
  public void close() {
    if (deserializer.respondsTo("close")) {
      deserializer.callMethod(runtime.getCurrentContext(), "close");
    }
  }
}
