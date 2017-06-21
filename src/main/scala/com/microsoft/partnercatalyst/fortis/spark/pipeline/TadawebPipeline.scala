package com.microsoft.partnercatalyst.fortis.spark.pipeline

import com.microsoft.partnercatalyst.fortis.spark.dto._
import com.microsoft.partnercatalyst.fortis.spark.streamprovider.{ConnectorConfig, StreamProvider}
import com.microsoft.partnercatalyst.fortis.spark.tadaweb.dto.TadawebEvent
import com.microsoft.partnercatalyst.fortis.spark.transforms.language.LanguageDetector
import com.microsoft.partnercatalyst.fortis.spark.transforms.locations.LocationsExtractor
import com.microsoft.partnercatalyst.fortis.spark.transforms.sentiment.SentimentDetector
import com.microsoft.partnercatalyst.fortis.spark.transforms.topic.KeywordExtractor
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class TadawebPipeline(
  @transient ssc: StreamingContext,
  @transient streamProvider: StreamProvider,
  @transient streamRegistry: Map[String, List[ConnectorConfig]],
  @transient transformContext: TransformContext) extends PipelineBase[TadawebEvent](transformContext) {

  override def sourceStream(): Option[DStream[TadawebEvent]] = streamProvider.buildStream[TadawebEvent](ssc, streamRegistry("tadaweb"))

  override def toSchema(item: TadawebEvent, locationsExtractor: LocationsExtractor): AnalyzedItem[TadawebEvent] = {
    AnalyzedItem(
      item,
      item.tada.name,
      sharedLocations = item.cities.flatMap(city =>
        city.coordinates match {
          case Seq(latitude, longitude) => locationsExtractor.fetch(latitude = latitude, longitude = longitude)
          case _ => None
        }).toList,
      analysis = Analysis()
    )
  }
  override def extractKeywords(item: TadawebEvent, keywordExtractor: KeywordExtractor): List[Tag] = {
    keywordExtractor.extractKeywords(item.text) ::: keywordExtractor.extractKeywords(item.title)
  }

  override def extractLocations(item: TadawebEvent, locationsExtractor: LocationsExtractor): List[Location] = {
    locationsExtractor.analyze(item.text).toList
  }

  override def detectLanguage(item: TadawebEvent, languageDetector: LanguageDetector): Option[String] = {
    languageDetector.detectLanguage(item.text)
  }

  override def detectSentiment(item: TadawebEvent, sentimentDetector: SentimentDetector): List[Double] = {
    val sentiment: Option[Double] = item.sentiment match {
      case "negative" => Some(SentimentDetector.Negative)
      case "neutral" => Some(SentimentDetector.Neutral)
      case "positive" => Some(SentimentDetector.Positive)
      case _ => sentimentDetector.detectSentiment(item.text, "TODO: move lang param to instance")
    }

    sentiment.toList
  }
}