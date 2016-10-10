package io.burt.kafka.clients;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import org.jcodings.Encoding;
import org.jruby.Ruby;
import org.jruby.RubyEncoding;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

public class RubyStringDeserializer implements Deserializer<IRubyObject> {
  protected Ruby runtime;
  protected Encoding encoding;

  @Override
  public void configure(Map<String, ?> config, boolean isKey) {
    runtime = (Ruby) config.get(KafkaClientsLibrary.RUNTIME_CONFIG);
    RubyEncoding configEncoding = (RubyEncoding) config.get(KafkaClientsLibrary.ENCODING_CONFIG);
    encoding = configEncoding == null ? null : configEncoding.getEncoding();
  }

  @Override
  public IRubyObject deserialize(String topic, byte[] data) {
    if (data == null) {
      return runtime.getNil();
    } else {
      Encoding actualEncoding = encoding == null ? runtime.getDefaultExternalEncoding() : encoding;
      return runtime.newString(new ByteList(data, actualEncoding));
    }
  }

  @Override
  public void close() { }
}
