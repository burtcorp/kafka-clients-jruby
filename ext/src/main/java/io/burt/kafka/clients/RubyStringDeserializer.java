package io.burt.kafka.clients;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import org.jruby.Ruby;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

public class RubyStringDeserializer implements Deserializer<IRubyObject> {
  private final Ruby runtime;

  public RubyStringDeserializer(Ruby runtime) {
    this.runtime = runtime;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) { }

  @Override
  public IRubyObject deserialize(String topic, byte[] data) {
    if (data == null) {
      return runtime.getNil();
    } else {
      return runtime.newString(new ByteList(data));
    }
  }

  @Override
  public void close() { }
}
