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

package org.apache.flink.table.calcite

import java.lang.Iterable
import java.util

import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalTableScan}
import org.apache.calcite.rex.RexNode
//import scala.collection.Iterable


import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.{AggCall, GroupKey}
import org.apache.calcite.util.Static.RESOURCE
import org.apache.calcite.util.Util
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.{Alias, ExpressionBridge, PlannerExpression, WindowProperty}
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.plan.QueryOperationConverter
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.logical.rel.{LogicalTableAggregate, LogicalWindowAggregate, LogicalWindowTableAggregate}
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.nodes.dataset.DataSetScan
import org.apache.flink.table.runtime.aggregate.AggregateUtil

import scala.collection.JavaConverters._
import scala.tools.nsc.interpreter.JList

/**
  * Flink specific [[RelBuilder]] that changes the default type factory to a [[FlinkTypeFactory]].
  */
class FlinkRelBuilder(
    context: Context,
    relOptCluster: RelOptCluster,
    relOptSchema: RelOptSchema,
    expressionBridge: ExpressionBridge[PlannerExpression])
  extends RelBuilder(
    context,
    relOptCluster,
    relOptSchema) {

  private val toRelNodeConverter = new QueryOperationConverter(this, expressionBridge)

  def getRelOptSchema: RelOptSchema = relOptSchema



  def getPlanner: RelOptPlanner = cluster.getPlanner

  def getCluster: RelOptCluster = relOptCluster

  override def shouldMergeProject(): Boolean = false

  override def getTypeFactory: FlinkTypeFactory =
    super.getTypeFactory.asInstanceOf[FlinkTypeFactory]

  var scanFactory = Util.first(context.unwrap(classOf[RelFactories.TableScanFactory]), RelFactories.DEFAULT_TABLE_SCAN_FACTORY)

  def scan(tableNames: Iterable[String], location: String): RelBuilder = {
    val names: util.List[String] = ImmutableList.copyOf(tableNames)
    val relOptTable: RelOptTable = relOptSchema.getTableForMember(names)
    if (relOptTable == null) throw RESOURCE.tableNotFound(String.join(".", names)).ex
    val scan: RelNode = scanFactory.createScan(cluster, relOptTable)
    scan.asInstanceOf[LogicalTableScan].setLocation(location);
    push(scan)
    rename(relOptTable.getRowType.getFieldNames)
    this
  }






  /**
    * Build non-window aggregate for either aggregate or table aggregate.
    */
  override def aggregate(groupKey: GroupKey, aggCalls: Iterable[AggCall]): RelBuilder = {
    // build a relNode, the build() may also return a project
    val relNode = super.aggregate(groupKey, aggCalls).build()

    relNode match {
      case logicalAggregate: LogicalAggregate
        if AggregateUtil.isTableAggregate(logicalAggregate.getAggCallList) =>
        push(LogicalTableAggregate.create(logicalAggregate))
      case _ => push(relNode)
    }
  }

  /**
    * Build window aggregate for either aggregate or table aggregate.
    */
  def windowAggregate(
      window: LogicalWindow,
      groupKey: GroupKey,
      windowProperties: JList[PlannerExpression],
      aggCalls: Iterable[AggCall])
    : RelBuilder = {
    // build logical aggregate
    val aggregate = super.aggregate(groupKey, aggCalls).build().asInstanceOf[LogicalAggregate]

    val namedProperties = windowProperties.asScala.map {
      case Alias(p: WindowProperty, name, _) =>
        p.toNamedWindowProperty(name)
      case _ => throw new TableException("This should never happen.")
    }

    // build logical window aggregate from it
    if (AggregateUtil.isTableAggregate(aggregate.getAggCallList)) {
      push(LogicalWindowTableAggregate.create(window, namedProperties, aggregate))
    } else {
      push(LogicalWindowAggregate.create(window, namedProperties, aggregate))
    }
  }

  def tableOperation(tableOperation: QueryOperation): RelBuilder= {
    val relNode = tableOperation.accept(toRelNodeConverter)

    push(relNode)
    this
  }

}

object FlinkRelBuilder {

  /**
    * Information necessary to create a window aggregate.
    *
    * Similar to [[RelBuilder.AggCall]] or [[RelBuilder.GroupKey]].
    */
  case class NamedWindowProperty(name: String, property: WindowProperty)

  def of(cluster: RelOptCluster, relOptSchema: RelOptSchema): FlinkRelBuilder = {
    val clusterContext = cluster.getPlanner.getContext

    new FlinkRelBuilder(
      clusterContext,
      cluster,
      relOptSchema,
      clusterContext.unwrap(classOf[ExpressionBridge[PlannerExpression]]))
  }

}
