package com.fps.omid.utils

import java.io.Closeable
import com.fps.omid.utils.resources._

import org.scalatest.FlatSpec

class DisposableTest extends FlatSpec {

  class DisposableObject(val s: String) {
    var closed = false
    println(s"Disposable object create ${s}")

    def doSomething = println(s"I'm disposable object ${s}")

    def dispose(): Unit = {
      println(s"Disposable object close ${s} previously closed=${closed}")
      closed = true
    }
  }

  class CloseableObject(val s: String) extends Closeable {
    var closed = false
    println(s"Closeable object create ${s}")

    def doSomething = println(s"I'm closeable object ${s}")

    override def close(): Unit = {
      println(s"Closeable object close ${s} previously closed=${closed}")
      closed = true
    }
  }

  class AutoCloseableObject(val s: String) extends AutoCloseable {
    var closed = false
    println(s"AutoCloseable object create ${s}")

    def doSomething = println(s"I'm autocloseable object ${s}")

    override def close(): Unit = {
      println(s"AutoCloseable object close ${s} previously closed=${closed}")
      closed = true
    }
  }

  "A Disposable object implementing dispose()" should "be closed after disposed" in {

    val resource = new DisposableObject("do 1")

    for {
      dispObj <- resource.autoDispose
    } {
      dispObj.doSomething
    }
    assert(resource.closed)
  }

  "A java.io.Closeable object" should "be closed after disposed" in {
    val co = new CloseableObject("co 1")
    assert(!co.closed)
    import com.fps.omid.utils.resources._
    for (r <- co.autoDispose) {
      r.doSomething
    }
    assert(co.closed)
  }

  "A java.io.AutoCloseable object" should "be closed after disposed" in {
    val aco = new AutoCloseableObject("aco 1")
    assert(!aco.closed)
    import com.fps.omid.utils.resources._
    for (r <- aco.autoDispose) {
      r.doSomething
    }
    assert(aco.closed)
  }

}