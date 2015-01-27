package com.twitter.scalding.bdd

import cascading.tuple.Tuple
import com.twitter.scalding.RichPipe
import org.specs.Specification
import scala.collection.mutable.Buffer
import scala.util.Try

class TrapSupportSpecTest extends Specification with BddDsl {

  "A test with single source and traps" should {
    "accept an operation with a single input rich pipe" in {
      Given {
        List(
          ("col1_1", "col2_1"),
          ("throw", "col2_2")) withSchema (('col1, 'col2))
      } When {
        pipe: RichPipe =>
          {
            pipe.map('col1 -> 'col1_transf) {
              col1: String =>
                require(col1 != "throw", "Test exception")
                col1 + "_transf"
            }
          }
      } Then {
        (output: Buffer[(String, String, String)], exception: Buffer[String]) =>

          output.toList mustEqual List(("col1_1", "col2_1", "col1_1_transf"))

          exception.toList mustEqual List("throw")
      }
    }

    "accept an operation with a single input rich pipe - fail if the validation function fails" in {
      val testResult = Try {
        Given {
          List(
            ("col1_1", "col2_1"),
            ("throw", "col2_2")) withSchema (('col1, 'col2))
        } When {
          pipe: RichPipe =>
            {
              pipe.map('col1 -> 'col1_transf) {
                col1: String =>
                  require(col1 != "throw", "Test exception")
                  col1 + "_transf"
              }
            }
        } Then {
          (output: Buffer[(String, String, String)], exception: Buffer[String]) =>

            output.toList mustEqual List(("col1_1", "col2_1", "col1_1_transf"), ("", "", ""))

            exception.toList mustEqual List("throw")
        }
      }

      testResult.isFailure must beTrue
    }
  }
}
