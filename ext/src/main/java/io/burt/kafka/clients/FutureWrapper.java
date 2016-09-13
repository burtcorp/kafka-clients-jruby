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
public class FutureWrapper<T> extends RubyObject implements Future<T> {
  private final Future<T> future;

  public FutureWrapper(Ruby runtime, RubyClass metaClass, Future<T> future) {
    super(runtime, metaClass);
    this.future = future;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass futureClass = parentModule.defineClassUnder("Future", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    futureClass.defineAnnotatedMethods(FutureWrapper.class);
    return futureClass;
  }

  static <U> FutureWrapper<U> create(Ruby runtime, Future<U> future) {
    return new FutureWrapper<U>(runtime, (RubyClass) runtime.getClassFromPath("Kafka::Clients::Future"), future);
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

  @Override
  public T get() throws InterruptedException, ExecutionException {
    return future.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeout, unit);
  }

  @JRubyMethod(name = "get", optional = 1)
  public IRubyObject getRb(ThreadContext ctx, IRubyObject[] args) throws InterruptedException {
    long timeout = -1;
    if (args.length > 0) {
      RubyHash options = args[0].convertToHash();
      IRubyObject timeoutOption = options.fastARef(ctx.runtime.newString("timeout"));
      if (timeoutOption!= null && !timeoutOption.isNil()) {
        timeout = (long) Math.floor(timeoutOption.convertToFloat().getDoubleValue() * 1000);
      }
    }
    try {
      if (timeout >= 0) {
        get(timeout, TimeUnit.MILLISECONDS);
        return ctx.runtime.getNil();
      } else {
        get();
        return ctx.runtime.getNil();
      }
    } catch (ExecutionException ee) {
      // TODO: fix some kind of exception mapper helper thingie
      RubyClass errorClass = (RubyClass) ctx.runtime.getClassFromPath("Kafka::Clients::KafkaError");
      throw ctx.runtime.newRaiseException(errorClass, ee.getCause().getMessage());
    } catch (TimeoutException te) {
      RubyClass errorClass = (RubyClass) ctx.runtime.getClassFromPath("Kafka::Clients::TimeoutError");
      throw ctx.runtime.newRaiseException(errorClass, te.getMessage());
    }
  }
}
