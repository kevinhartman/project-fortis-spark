package com.microsoft.partnercatalyst.fortis.spark.logging

import com.microsoft.applicationinsights.{TelemetryClient, TelemetryConfiguration}
import java.util.HashMap

class AppInsightsTelemetry extends FortisTelemetry {
  private val client: TelemetryClient = new TelemetryClient(TelemetryConfiguration.createDefault())

  def logIncomingEventBatch(streamId: String, connectorName: String, batchSize: Long): Unit = {
    val properties = new HashMap[String, String](2)
    properties.put("streamId", streamId)
    properties.put("connectorName", connectorName)

    val metrics = new HashMap[String, java.lang.Double](1)
    metrics.put("batchSize", batchSize.toDouble)

    client.trackEvent("batch.receive", properties, metrics)
  }

  def logCassandraEventsSink(duration: Long, batchSize: Long): Unit = {
    val properties = new HashMap[String, String](0)

    val metrics = new HashMap[String, java.lang.Double](2)
    metrics.put("batchSize", batchSize.toDouble)
    metrics.put("duration", duration.toDouble)

    client.trackEvent("batch.sink", properties, metrics)
  }
}