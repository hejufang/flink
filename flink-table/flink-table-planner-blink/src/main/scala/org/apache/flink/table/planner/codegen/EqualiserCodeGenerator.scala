/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.codegen

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens.generateEquals
import org.apache.flink.table.runtime.generated.{GeneratedRecordEqualiser, RecordEqualiser}
import org.apache.flink.table.types.logical.{LogicalType, RowType}

class EqualiserCodeGenerator(fieldTypes: Array[LogicalType]) {

  private val RECORD_EQUALISER = className[RecordEqualiser]
  private val LEFT_INPUT = "left"
  private val RIGHT_INPUT = "right"

  def generateRecordEqualiser(name: String): GeneratedRecordEqualiser = {
    // ignore time zone
    val ctx = CodeGeneratorContext(new TableConfig)
    val className = newName(name)
    val rowType = RowType.of(fieldTypes:_*)
    val left = GeneratedExpression(LEFT_INPUT, NEVER_NULL, "", rowType)
    val right = GeneratedExpression(RIGHT_INPUT, NEVER_NULL, "", rowType)
    val gen = generateEquals(ctx, left, right)
    val functionCode =
      j"""
         |public final class $className implements $RECORD_EQUALISER {
         |  ${ctx.reuseMemberCode()}
         |
         |  public $className(Object[] references) throws Exception {
         |    ${ctx.reuseInitCode()}
         |  }
         |
         |  @Override
         |  public boolean equals($ROW_DATA $LEFT_INPUT, $ROW_DATA $RIGHT_INPUT) {
         |    ${ctx.reuseLocalVariableCode()}
         |    ${gen.code}
         |    return ${gen.resultTerm};
         |  }
         |}
      """.stripMargin

    new GeneratedRecordEqualiser(className, functionCode, ctx.references.toArray, ctx.tableConfig)
  }
}
