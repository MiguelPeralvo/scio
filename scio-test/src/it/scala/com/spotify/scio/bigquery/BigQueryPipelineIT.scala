/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.bigquery

import com.google.cloud.dataflow.sdk.options.{GcpOptions, PipelineOptionsFactory}
import com.spotify.scio._
import com.spotify.scio.experimental._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection
import org.scalatest.Matchers


object BigQueryPipelineIT {
  @BigQueryType.fromQuery(
    """|SELECT word, corpus
       |FROM `data-integration-test.samples_us.shakespeare`""".stripMargin)
  class WordCountShakespeare
}

class BigQueryPipelineIT extends PipelineSpec with Matchers {
  import BigQueryPipelineIT._

  // Need to set project to make BigQueryIO happy
  private def runLocalWithProject[U](project: String)(fn: ScioContext => SCollection[U]): Seq[U] = {
    val opts = PipelineOptionsFactory.create().as(classOf[GcpOptions])
    opts.setProject(project)

    val sc = ScioContext(opts)
    val f = fn(sc).materialize
    sc.close()
    f.waitForResult().value.toSeq
  }

  def runLocalWithIt[U]: (ScioContext => SCollection[U]) => Seq[U] =
    runLocalWithProject("data-integration-test")

  "typedBigQuery" should "support embedded query" in {
    runLocalWithIt { sc: ScioContext =>
      sc.typedBigQuery[WordCountShakespeare]().count
    }.head shouldBe 164656L
  }

  // scalastyle:off line.size.limit
  it should "support different table with the same schema" in {
    runLocalWithIt { sc =>
      sc.typedBigQuery[WordCountShakespeare]("data-integration-test:samples_us.shakespeare_copy_nullable")
        .count
    }.head shouldBe 164656L
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "throw an exception on different table with incompatible schema" in {
    val ex = the [IllegalArgumentException] thrownBy {
      runLocalWithIt { sc =>
        sc.typedBigQuery[WordCountShakespeare]("data-integration-test:samples_us.shakespeare_altered")
          .count
      }
    }
    ex.getMessage should startWith ("requirement failed: New source")
  }
  // scalastyle:on no.whitespace.before.left.bracket
  // scalastyle:on line.size.limit

  it should "support different query with the same schema" in {
    runLocalWithIt { sc =>
      sc.typedBigQuery[WordCountShakespeare](
        """|SELECT word, corpus
           |FROM `data-integration-test.samples_us.shakespeare_copy`""".stripMargin)
        .count
    }.head shouldBe 164656L
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "throw an exception on different query with incompatible schema" in {
    val ex = the [IllegalArgumentException] thrownBy {
      runLocalWithIt { sc =>
        sc.typedBigQuery[WordCountShakespeare](
          """|SELECT word
             |FROM `data-integration-test.samples_us.shakespeare_copy`""".stripMargin)
          .count
      }
    }
    ex.getMessage should startWith ("requirement failed: New source")
  }
  // scalastyle:on no.whitespace.before.left.bracket
}