package com.fps.omid.utils

import java.io.Closeable

import scala.util.{DynamicVariable, Failure, Try}
import scala.util.control.NonFatal
import scala.language.{implicitConversions, reflectiveCalls}

trait Disposable[R] {
  def autoDispose: DisposableResource[R]
}

abstract class DisposableResource[R](private val value: R) extends Disposable[R] {

  protected def close(value: R): Unit

  final override def autoDispose: DisposableResource[R] = this

  final def flatMap[B](f: (R) => B): B = map(f)

  final def foreach[U](f: (R) => U): Unit = map(f)

  final def map[B](f: (R) => B): B = {
    try {
      f(value)
    } finally {
      try {
        close(value)
      } catch {
        case NonFatal(e) => DisposableResource.exceptionHandler.value(e)
      }
    }
  }

}

object DisposableResource {
  val exceptionHandler = new DynamicVariable[(Throwable => Unit)]({ t =>})
}

package object resources {

  implicit def closeMethod[R <: {def close() : Any}](r: R): Disposable[R] = r match {

    case c: Closeable => resources.closeableResource(c).asInstanceOf[Disposable[R]]

    case c: AutoCloseable => resources.autoCloseableResource(c).asInstanceOf[Disposable[R]]

    case _ => {
      new DisposableResource[R](r) {
        override protected def close(value: R) {
          value.close()
        }
      }
    }
  }

  implicit def disposeMethod[R <: {def dispose() : Any}](r: R): Disposable[R] = { println("aaaa") ; new DisposableResource[R](r) {
    override protected def close(value: R) {
      value.dispose()
    }
  }}

  implicit def closeableResource[R <: Closeable](r: R): Disposable[R] = { println("bbbb") ; new DisposableResource[R](r) {
    override protected def close(value: R) {
      value.close()
    }
  }}

  implicit def autoCloseableResource[R <: AutoCloseable](r: R): Disposable[R] = { println("cccc") ; new DisposableResource[R](r) {
    override protected def close(value: R) {
      value.close()
    }
  }}

}