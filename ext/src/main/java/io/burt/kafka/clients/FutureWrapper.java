package io.burt.kafka.clients;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@SuppressWarnings("serial")
@JRubyClass(name = "Kafka::Clients::Future")
public class FutureWrapper<T> extends RubyObject implements Future<IRubyObject> {
  private final Future<T> future;
  private final Rubifier<T> rubifier;

  public FutureWrapper(Ruby runtime, RubyClass metaClass, Future<T> future, Rubifier<T> transformer) {
    super(runtime, metaClass);
    this.future = future;
    this.rubifier = transformer;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass cls = parentModule.defineClassUnder("Future", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    cls.defineAnnotatedMethods(FutureWrapper.class);
    return cls;
  }

  static <V> FutureWrapper<V> create(Ruby runtime, Future<V> future, Rubifier<V> transformer) {
    return new FutureWrapper<>(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::Future"), future, transformer);
  }

  public interface Rubifier<V> {
    IRubyObject transform(V value);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return future.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return future.isCancelled();
  }

  @Override
  public boolean isDone() {
    return future.isDone();
  }

  @JRubyMethod(name = "done?")
  public IRubyObject isDoneRb(ThreadContext ctx) {
    return ctx.runtime.newBoolean(isDone());
  }

  @Override
  public IRubyObject get() throws InterruptedException, ExecutionException {
    return rubifier.transform(future.get());
  }

  @Override
  public IRubyObject get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return rubifier.transform(future.get(timeout, unit));
  }

  @JRubyMethod(name = "get", optional = 1)
  public IRubyObject getRb(ThreadContext ctx, IRubyObject[] args) throws InterruptedException {
    long timeout = -1;
    if (args.length > 0) {
      RubyHash options = args[0].convertToHash();
      IRubyObject timeoutOption = options.fastARef(ctx.runtime.newSymbol("timeout"));
      if (timeoutOption!= null && !timeoutOption.isNil()) {
        timeout = (long) Math.floor(timeoutOption.convertToFloat().getDoubleValue() * 1000);
      }
    }
    try {
      if (timeout >= 0) {
        return get(timeout, TimeUnit.MILLISECONDS);
      } else {
        return get();
      }
    } catch (ExecutionException ee) {
      throw KafkaClientsLibrary.newRaiseException(ctx.runtime, ee.getCause());
    } catch (TimeoutException te) {
      throw KafkaClientsLibrary.newRaiseException(ctx.runtime, te);
    }
  }
}
