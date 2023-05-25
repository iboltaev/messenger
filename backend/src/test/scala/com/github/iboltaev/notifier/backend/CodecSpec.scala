package com.github.iboltaev.notifier.backend

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.jdk.CollectionConverters._

class CodecSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  import com.github.ibolteav.notifier.backend.hbase.bindings.Codecs._

  implicit def asTraversable[T](ar: java.util.ArrayList[T]): Traversable[T] =
    ar.asScala

  case class Ex1(v1: Long, v2: String, v3: Ex2, v4: Long)
  case class Ex2(v5: Long, v6: String)

  case class Ex3(key: String, value: Either[Ex1, Ex2])

  lazy val keyCodec = KeyCodec.gen[Ex1]
  lazy val valueCodec = ValueCodec.gen[Ex1]
  lazy val valCodec = ValueCodec.gen[Either[Ex1, Ex2]]
  lazy val valCodec2 = ValueCodec.gen[Ex3]

  implicit lazy val ordering: Ordering[Ex1] = Ordering.by[Ex1, (Long, String, Long, String, Long)](v => (v.v1, v.v2, v.v3.v5, v.v3.v6, v.v4))

  lazy val gen = for {
    i <- Gen.choose(0, 100500)
    str <- Gen.alphaNumStr
    j <- Gen.choose(0, 100500)
    k <- Gen.choose(0, 100500)
    str2 <- Gen.alphaStr
  } yield Ex1(i, str, Ex2(j, str2), k)

  it should "work with KeyCodec" in {
    forAll(gen) { ex =>
      val str2 = keyCodec.encode(ex)
      val ex2 = keyCodec.decode(str2)

      ex2 should equal (ex)
    }
  }

  it should "work with ValueCodec" in {
    forAll(gen) { ex =>
      val map = valueCodec.encodeMap(ex)
      val ex2 = valueCodec.decodeMap(map)

      ex2 should equal (ex)
    }
  }

  "KeyCodec" should "preserve ordering" in {
    val seqGen = Gen.containerOf(gen)

    forAll(seqGen) { seq =>
      seq.toSeq.sorted
      val ordered = seq.toSeq.sorted
      val parsed = seq.map(s => keyCodec.encode(s)).toSeq.sorted

      parsed.map(keyCodec.decode) should contain theSameElementsInOrderAs(ordered)
    }
  }

  // TODO: make normal property tests!
  "ValueCodec" should "work with sealed trait" in {
    val ex1 = Ex1(1, "str1", Ex2(2, "str2"), 3)
    val ex2 = Ex2(4, "str3")

    val eith1: Either[Ex1, Ex2] = Left(ex1)
    val eith2: Either[Ex1, Ex2] = Right(ex2)

    val map1 = valCodec.encodeMap(eith1)
    println(map1)

    val res1 = valCodec.decodeMap(map1)
    println(res1)

    val map2 = valCodec.encodeMap(eith2)
    println(map2)

    val res2 = valCodec.decodeMap(map2)
    println(res2)
  }

  it should "work with sealed trait in case class" in {
    val ex1 = Ex1(1, "str1", Ex2(2, "str2"), 3)
    val ex3 = Ex3("key", Left(ex1))

    val map = valCodec2.encodeMap(ex3)
    println(map)

    val res = valCodec2.decodeMap(map)
    println(res)
  }
}
