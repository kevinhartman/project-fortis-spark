package com.microsoft.partnercatalyst.fortis.spark.transforms.entities

import java.io.{IOError, IOException}

import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.microsoft.partnercatalyst.fortis.spark.transforms.ZipModelsProvider
import com.microsoft.partnercatalyst.fortis.spark.transforms.nlp.OpeNER
import ixa.kaflib.{Entity, Term}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

@SerialVersionUID(100L)
class EntityRecognizer(
  modelsProvider: ZipModelsProvider,
  language: Option[String]
) extends Serializable with Loggable {

  def extractTerms(text: String): List[Term] = {
    if (!isValid) {
      return List()
    }

    Try(modelsProvider.ensureModelsAreDownloaded(language.get)) match {
      case Failure(ex) =>
        logError(s"Unable to load models for language $language", ex)
        List()

      case Success(resourcesDirectory) =>
        extractTermsUsingModels(text, resourcesDirectory)
    }
  }

  private def extractTermsUsingModels(text: String, resourcesDirectory: String): List[Term] = {
    try {
      val kaf = OpeNER.tokAnnotate(resourcesDirectory, text, language.get)
      OpeNER.posAnnotate(resourcesDirectory, language.get, kaf)
      OpeNER.nerAnnotate(resourcesDirectory, language.get, kaf)

      kaf.getTerms.toList
    } catch {
      case ex @ (_ : NullPointerException | _ : IOError | _ : IOException) =>
        logError(s"Unable to extract entities for language $language", ex)
        List()
    }
  }

  def extractEntities(text: String): List[Entity] = {
    if (!isValid) {
      return List()
    }

    Try(modelsProvider.ensureModelsAreDownloaded(language.get)) match {
      case Failure(ex) =>
        logError(s"Unable to load models for language $language", ex)
        List()

      case Success(resourcesDirectory) =>
        extractEntitiesUsingModels(text, resourcesDirectory)
    }
  }

  def isValid: Boolean = language.isDefined && OpeNER.EnabledLanguages.contains(language.get)

  private def extractEntitiesUsingModels(text: String, resourcesDirectory: String): List[Entity] = {
    try {
      val kaf = OpeNER.tokAnnotate(resourcesDirectory, text, language.get)
      OpeNER.posAnnotate(resourcesDirectory, language.get, kaf)
      OpeNER.nerAnnotate(resourcesDirectory, language.get, kaf)

      kaf.getEntities.toList
    } catch {
      case ex @ (_ : NullPointerException | _ : IOError | _ : IOException) =>
        logError(s"Unable to extract entities for language $language", ex)
        List()
    }
  }
}
