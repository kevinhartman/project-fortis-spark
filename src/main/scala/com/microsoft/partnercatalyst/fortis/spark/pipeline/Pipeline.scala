package com.microsoft.partnercatalyst.fortis.spark.pipeline

import com.microsoft.partnercatalyst.fortis.spark.dto._
import com.microsoft.partnercatalyst.fortis.spark.transforms.language.LanguageDetector
import com.microsoft.partnercatalyst.fortis.spark.transforms.locations.LocationsExtractor
import com.microsoft.partnercatalyst.fortis.spark.transforms.sentiment.SentimentDetector
import com.microsoft.partnercatalyst.fortis.spark.transforms.topic.KeywordExtractor
import org.apache.spark.streaming.dstream.DStream

trait Pipeline {
  def output: Option[DStream[FortisItem]]
}

abstract class PipelineBase[T](@transient transformContext: TransformContext) extends Pipeline {

  def sourceStream(): Option[DStream[T]]

  def toSchema(item: T, locationsExtractor: LocationsExtractor): AnalyzedItem[T]
  def extractKeywords(item: T, keywordExtractor: KeywordExtractor): List[Tag]
  def extractLocations(item: T, locationsExtractor: LocationsExtractor): List[Location]
  def detectLanguage(item: T, languageDetector: LanguageDetector): Option[String]
  def detectSentiment(item: T, sentimentDetector: SentimentDetector): List[Double]

  def output: Option[DStream[FortisItem]] = {

    val _transformContext = transformContext

    sourceStream() match {
      case Some(stream) => Some(stream.transform(rdd => rdd
        .map(toSchema(_, _transformContext.locationsExtractor))
        .map(analyzedItem => analyzedItem.copy(analysis = Analysis(language = detectLanguage(analyzedItem.originalItem, _transformContext.languageDetector)))        )
        .filter(analyzedItem =>
            // restrict to supported languages
            analyzedItem.analysis.language match {
              case None => false
              case Some(language) => _transformContext.supportedLanguages.contains(language)
            }
        )
        .map(analyzedItem => {
          // keywords extraction
          analyzedItem.copy(analysis = analyzedItem.analysis.copy(keywords = extractKeywords(analyzedItem.originalItem, _transformContext.keywordExtractor)))
        })
        .map(analyzedItem => {
          // analyze sentiment
          // TODO: add sentiment detector factory to TransformContext that can produce a sentiment detector for the language
          // since implementors don't know the language within detectSentiment().
          analyzedItem.copy(analysis = analyzedItem.analysis.copy(sentiments = detectSentiment(analyzedItem.originalItem, _transformContext.sentimentDetector)))
        })
        .map(analyzedItem => {
          // infer locations from text
          // TODO: add location detector factory ""
          analyzedItem.copy(analysis = analyzedItem.analysis.copy(locations = extractLocations(analyzedItem.originalItem, _transformContext.locationsExtractor)))
        })))
      case None => None
    }
  }
}
