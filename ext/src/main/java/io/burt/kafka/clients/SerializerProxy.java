package io.burt.kafka.clients;

import java.util.Map;

import org.jruby.runtime.builtin.IRubyObject;

public class SerializerProxy extends RubyStringSerializer {
  private IRubyObject serializer;
  private String serializeMethodName;

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    super.configure(config, isKey);
    serializer = (IRubyObject) config.get(KafkaClientsLibrary.SERIALIZER_CONFIG_PREFIX + (isKey ? "key" : "value"));
    if (serializer.respondsTo("serialize")) {
      serializeMethodName = "serialize";
    } else if (serializer.respondsTo("call")) {
      serializeMethodName = "call";
    } else {
      throw runtime.newTypeError(serializer, "object that responds to #serialize or #call");
    }
  }

  @Override
  public byte[] serialize(String topic, IRubyObject data) {
    if (data == null) {
      return super.serialize(topic, data);
    } else {
      return super.serialize(topic, serializer.callMethod(runtime.getCurrentContext(), serializeMethodName, data));
    }
  }

  @Override
  public void close() {
    if (serializer.respondsTo("close")) {
      serializer.callMethod(runtime.getCurrentContext(), "close");
    }
  }
}
