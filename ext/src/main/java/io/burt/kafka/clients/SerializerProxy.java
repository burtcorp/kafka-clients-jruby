package io.burt.kafka.clients;

import java.util.Map;

import org.jruby.runtime.builtin.IRubyObject;

public class SerializerProxy extends RubyStringSerializer {
  private IRubyObject serializer;

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    super.configure(config, isKey);
    serializer = (IRubyObject) config.get(KafkaClientsLibrary.SERIALIZER_CONFIG_PREFIX + (isKey ? "key" : "value"));
  }

  @Override
  public byte[] serialize(String topic, IRubyObject data) {
    if (data == null) {
      return super.serialize(topic, data);
    } else {
      return super.serialize(topic, serializer.callMethod(runtime.getCurrentContext(), "serialize", data));
    }
  }

  @Override
  public void close() {
    if (serializer.respondsTo("close")) {
      serializer.callMethod(runtime.getCurrentContext(), "close");
    }
  }
}
