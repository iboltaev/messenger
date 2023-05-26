package com.github.iboltaev.notifier.backend.hbase.bindings

import magnolia1.{CaseClass, Magnolia, SealedTrait}
import org.apache.hadoop.hbase.client.{Delete, Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes

import scala.jdk.CollectionConverters._

object Codecs {
  implicit def toBytes(s: String): Array[Byte] = Bytes.toBytes(s)

  trait KeyCodec[K] {
    val size: Int = 1

    def encodeVec(k: K): Vector[String]
    def decodeVec(v: Vector[String], offset: Int = 0): K
  }

  implicit val strKeyCodec: KeyCodec[String] = new KeyCodec[String] {
    override def encodeVec(k: String): Vector[String] = Vector(if (k == null) "" else k)
    override def decodeVec(v: Vector[String], offset: Int): String = {
      val str = v(offset)
      if (str == null) "" else str
    }
  }

  implicit val longKeyCodec: KeyCodec[Long] = new KeyCodec[Long] {
    override def encodeVec(k: Long): Vector[String] = {
      val sign = if (k < 0) 'm' else if (k == 0) 'o' else 'p'
      val value = k.toString
      val size = String.format("%02X", value.length)
      Vector(sign + size + value)
    }

    override def decodeVec(v: Vector[String], offset: Int): Long = {
      val str = v(offset)
      if (str == null || str.isEmpty || str.charAt(0) == 'o') 0L
      else {
        val value = str.substring(3).toLong
        if (str.charAt(0) == 'm') -value else value
      }
    }
  }

  object KeyCodec {
    type Typeclass[K] = KeyCodec[K]

    def join[K](ctx: CaseClass[KeyCodec, K]): KeyCodec[K] = new KeyCodec[K] {
      override val size = ctx.parameters.map(_.typeclass.size).sum

      override def encodeVec(k: K): Vector[String] = ctx.parameters.foldLeft(Vector.empty[String]) { (v, p) =>
        v ++ p.typeclass.encodeVec(p.dereference(k))
      }

      override def decodeVec(s: Vector[String], offset: Int): K = {
        val offsets = ctx.parameters.scanLeft(offset)(_ + _.typeclass.size).toArray
        ctx.construct { p =>
          p.typeclass.decodeVec(s, offsets(p.index))
        }
      }
    }

    def split[K](ctx: SealedTrait[KeyCodec, K]): KeyCodec[K] = new KeyCodec[K] {
      override def encodeVec(k: K): Vector[String] = ???
      override def decodeVec(v: Vector[String], offset: Int): K = ???
    }

    implicit def gen[K]: KeyCodec[K] = macro Magnolia.gen[K]
  }

  trait ValueCodec[V] {
    def encodeMap(v: V, param: String = ""): Map[String, String]
    def decodeMap(m: Map[String, String], param: String = ""): V
  }

  def mkStrValueCodec(paramName: String) = new ValueCodec[String] {
    override def encodeMap(v: String, param: String): Map[String, String] = Map(paramName -> v)
    override def decodeMap(m: Map[String, String], param: String): String = m(paramName)
  }

  implicit val strValueCodec: ValueCodec[String] = new ValueCodec[String] {
    override def encodeMap(v: String, param: String): Map[String, String] = Map((param -> v))
    override def decodeMap(m: Map[String, String], param: String): String = m(param)
  }

  implicit val longValueCodec: ValueCodec[Long] = new ValueCodec[Long] {
    override def encodeMap(v: Long, param: String): Map[String, String] = Map((param -> v.toString))
    override def decodeMap(m: Map[String, String], param: String): Long = m(param).toLong
  }

  object ValueCodec {
    type Typeclass[V] = ValueCodec[V]

    def join[V](ctx: CaseClass[ValueCodec, V]): ValueCodec[V] = new ValueCodec[V] {
      override def encodeMap(v: V, param: String = ""): Map[String, String] = {
        val maps = ctx.parameters.map { p =>
          p.typeclass.encodeMap(p.dereference(v), p.label)
        }
        maps.foldLeft(Map.empty[String, String]) { (s, m) =>
          m.foldLeft(s) { (s, p) =>
            if (s.contains(p._1)) throw new RuntimeException(s"key ${p._1} already exists")
            else s.updated(p._1, p._2)
          }
        }
      }

      override def decodeMap(m: Map[String, String], param: String = ""): V = {
        ctx.construct { p =>
          p.typeclass.decodeMap(m, p.label)
        }
      }
    }

    def split[V](ctx: SealedTrait[ValueCodec, V]): ValueCodec[V] = new ValueCodec[V] {
      override def encodeMap(v: V, param: String = ""): Map[String, String] = {
        ctx.split(v) { st =>
          val paramName = if (param.isEmpty) "typ" else param
          st.typeclass.encodeMap(st.cast(v)) ++ Map(paramName -> st.typeName.short)
        }
      }

      override def decodeMap(m: Map[String, String], param: String = ""): V = {
        val typ = m.getOrElse(param, m("typ"))
        ctx.subtypes.find(_.typeName.short == typ).get.typeclass.decodeMap(m)
      }
    }

    implicit def gen[V]: ValueCodec[V] = macro Magnolia.gen[V]
  }
}
