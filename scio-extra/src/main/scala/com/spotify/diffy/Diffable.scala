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

import java.io.StringReader

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.GeneratedMessage
import com.spotify.ratatool.diffy.{AvroDiffy, Delta, ProtoBufDiffy, TableRowDiffy}
import org.apache.avro.generic.GenericRecord

import scala.reflect.ClassTag

trait Diffable[T] extends Serializable {
  val keyFn: T => String
  def deltas(x: T, y: T): Seq[Delta]
}

class AvroDiffable[T <: GenericRecord](override val keyFn: T => String) extends Diffable[T] {
  override def deltas(x: T, y: T): Seq[Delta] = AvroDiffy(x, y)
}

class ProtoBufDiffable[T <: GeneratedMessage : ClassTag](override val keyFn: T => String)
  extends Diffable[T] {
  // Descriptor and FieldDescriptor are not serializable
  private lazy val descriptor: Descriptor =
    implicitly[ClassTag[T]].runtimeClass
      .getMethod("getDescriptor")
      .invoke(null).asInstanceOf[Descriptor]

  override def deltas(x: T, y: T): Seq[Delta] = ProtoBufDiffy(x, y, descriptor)
}

class TableRowDiffable(override val keyFn: TableRow => String,
                       tableSchema: TableSchema) extends Diffable[TableRow] {
  // TableSchema is not serializable
  private val schemaString: String =
    new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
      .writeValueAsString(tableSchema)
  private lazy val schema: TableSchema =
    new JsonObjectParser(new JacksonFactory)
      .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

  override def deltas(x: TableRow, y: TableRow): Seq[Delta] = TableRowDiffy(x, y, schema)
}
