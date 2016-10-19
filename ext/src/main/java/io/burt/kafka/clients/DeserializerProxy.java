package io.burt.kafka.clients;

import java.util.Map;

import org.jruby.runtime.builtin.IRubyObject;

public class DeserializerProxy extends RubyStringDeserializer {
  private IRubyObject deserializer;
  private String deserializeMethodName;

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    super.configure(config, isKey);
    deserializer = (IRubyObject) config.get(KafkaClientsLibrary.DESERIALIZER_CONFIG_PREFIX + (isKey ? "key" : "value"));
    if (deserializer.respondsTo("deserialize")) {
      deserializeMethodName = "deserialize";
    } else if (deserializer.respondsTo("call")) {
      deserializeMethodName = "call";
    } else {
      throw runtime.newTypeError(deserializer, "object that responds to #deserialize or #call");
    }
  }

  @Override
  public IRubyObject deserialize(String topic, byte[] data) {
    if (data == null) {
      return super.deserialize(topic, data);
    } else {
      return deserializer.callMethod(runtime.getCurrentContext(), deserializeMethodName, super.deserialize(topic, data));
    }
  }

  @Override
  public void close() {
    if (deserializer.respondsTo("close")) {
      deserializer.callMethod(runtime.getCurrentContext(), "close");
    }
  }
}
