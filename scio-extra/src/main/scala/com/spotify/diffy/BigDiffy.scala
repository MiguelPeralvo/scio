/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.diffy

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.protobuf.GeneratedMessage
import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

object DiffType extends Enumeration {
  val SAME, DIFFERENT, MISSING_LHS, MISSING_RHS = Value
}

case class KeyStats(key: String, diffType: DiffType.Value) {
  override def toString: String = s"$key\t$diffType"
}

case class FieldStats(field: String,
                      numDiffRecords: Long,
                      numTotalRecords: Long,
                      fraction: Double) {
  override def toString: String = s"field\t$numDiffRecords\t$numTotalRecords\t$fraction"
}

case class DiffResult(keyStats: SCollection[KeyStats],
                      fieldStats: SCollection[FieldStats])

object BigDiffy {

  def diff[T](lhs: SCollection[T], rhs: SCollection[T], d: Diffable[T]): DiffResult = {
    val lKeyed = lhs.map(t => (d.keyFn(t), ("l", t)))
    val rKeyed = lhs.map(t => (d.keyFn(t), ("r", t)))

    val deltas = (lKeyed ++ rKeyed)
      .groupByKey
      .map { case (k, vs) =>
          val m = vs.toMap
          if (m.size == 2) {
            val ds = d.deltas(m("l"), m("r"))
            val dt = if (ds.isEmpty) DiffType.SAME else DiffType.DIFFERENT
            (k, (ds, dt))
          } else {
            val dt = if (m.contains("l")) DiffType.MISSING_RHS else DiffType.MISSING_LHS
            (k, (Nil, dt))
          }
      }

    val keyStats = deltas
      .filter(_._2._1 == DiffType.SAME)
      .map { case (k, (_, dt)) =>
        KeyStats(k, dt)
      }

    val fieldStats = deltas
      .map { case (_, (ds, dt)) =>
        val m = mutable.Map.empty[String, Long]
        ds.foreach(d => m(d.field) = 1L)
        (1L, m.toMap)
      }
      .sum
      .flatMap { case (total, fieldMap) =>
        fieldMap.map { case (field, count) =>
          FieldStats(field, count, total, count.toDouble / total)
        }
      }

    DiffResult(keyStats, fieldStats)
  }

  def diffAvro[T <: GenericRecord : ClassTag](sc: ScioContext,
                                              lhs: String, rhs: String,
                                              keyFn: T => String,
                                              schema: Schema = null): DiffResult =
    diff(sc.avroFile[T](lhs, schema), sc.avroFile[T](rhs, schema), new AvroDiffable[T](keyFn))

  def diffProtoBuf[T <: GeneratedMessage : ClassTag](sc: ScioContext,
                                                     lhs: String, rhs: String,
                                                     keyFn: T => String): DiffResult =
    diff(sc.protobufFile(lhs), sc.protobufFile(rhs), new ProtoBufDiffable[T](keyFn))

  def diffTableRow(sc: ScioContext,
                   lhs: String, rhs: String,
                   keyFn: TableRow => String): DiffResult = {
    val bq = BigQueryClient.defaultInstance()
    val lSchema = bq.getTableSchema(lhs)
    val rSchema = bq.getTableSchema(rhs)
    val schema = mergeTableSchema(lSchema, rSchema)
    diff(sc.bigQueryTable(lhs), sc.bigQueryTable(rhs), new TableRowDiffable(keyFn, schema))
  }

  private def mergeTableSchema(x: TableSchema, y: TableSchema): TableSchema = {
    val r = new TableSchema
    r.setFields(mergeFields(x.getFields.asScala, y.getFields.asScala).asJava)
  }

  private def mergeFields(x: Seq[TableFieldSchema],
                          y: Seq[TableFieldSchema]): Seq[TableFieldSchema] = {
    val xMap = x.map(f => (f.getName, f)).toMap
    val yMap = x.map(f => (f.getName, f)).toMap
    val names = mutable.LinkedHashSet.empty[String]
    xMap.foreach(kv => names.add(kv._1))
    yMap.foreach(kv => names.add(kv._1))
    names
      .map { n =>
        (xMap.get(n), yMap.get(n)) match {
          case (Some(f), None) => f
          case (None, Some(f)) => f
          case (Some(fx), Some(fy)) =>
            assert(fx.getType == fy.getType && fx.getMode == fy.getMode)
            if (fx.getType == "RECORD") {
              fx.setFields(mergeFields(fx.getFields.asScala, fy.getFields.asScala).asJava)
            } else {
              fx
            }
          case _ => throw new RuntimeException
        }
      }
      .toSeq
  }

}

object BQBigDiffy {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val lhs = "gabocontinuousintegration:neville_test_eu.ae71a"
    val rhs = "gabocontinuousintegration:neville_test_eu.ae91a"
    val result = BigDiffy.diffTableRow(sc, lhs, rhs, _.get("artist_gid").toString)
    result.keyStats.saveAsTextFile("gs://neville-gabo-eu/diff-keys")
    result.fieldStats.saveAsTextFile("gs://neville-gabo-eu/diff-fields")
    sc.close()
  }
}
