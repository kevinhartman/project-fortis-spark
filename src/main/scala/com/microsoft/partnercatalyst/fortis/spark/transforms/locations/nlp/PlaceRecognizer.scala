package com.microsoft.partnercatalyst.fortis.spark.transforms.locations.nlp

import java.io.{File, FileNotFoundException, IOError}
import java.net.URL
import java.nio.file.Files

import com.microsoft.partnercatalyst.fortis.spark.transforms.locations.Logger
import ixa.kaflib.Entity
import net.lingala.zip4j.core.ZipFile

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.sys.process._

@SerialVersionUID(100L)
class PlaceRecognizer(
  modelsSource: Option[String] = None,
  enabledLanguages: Set[String] = Set("de", "en", "es", "eu", "it", "nl")
) extends Serializable with Logger {

  @volatile private lazy val modelDirectories = mutable.Map[String, String]()

  def extractPlaces(text: String, language: String): Iterable[String] = {
    if (!enabledLanguages.contains(language)) {
      return Set()
    }

    try {
      val resourcesDirectory = ensureModelsAreDownloaded(language)

      var kaf = OpeNER.tokAnnotate(resourcesDirectory, text, language)
      OpeNER.posAnnotate(resourcesDirectory, language, kaf)
      OpeNER.nerAnnotate(resourcesDirectory, language, kaf)

      logDebug(s"Analyzed text $text in language $language: $kaf")

      kaf.getEntities.toList.filter(entityIsPlace).map(_.getStr).toSet
    } catch {
      case ex @ (_ : NullPointerException | _ : IOError) =>
        logError(s"Unable to extract places for language $language", ex)
        Set()
    }
  }

  private def entityIsPlace(entity: Entity) = {
    val entityType = Option(entity.getType).getOrElse("").toLowerCase
    entityType == "location" || entityType == "gpe"
  }

  private def ensureModelsAreDownloaded(language: String): String = {
    val localPath = modelsSource.getOrElse("")
    if (hasModelFiles(localPath, language)) {
      logDebug(s"Using locally provided model files from $localPath")
      modelDirectories.putIfAbsent(language, localPath)
      return localPath
    }

    val previouslyDownloadedPath = modelDirectories.getOrElse(language, "")
    if (hasModelFiles(previouslyDownloadedPath, language)) {
      logDebug(s"Using previously downloaded model files from $previouslyDownloadedPath")
      return previouslyDownloadedPath
    }

    val remotePath = modelsSource.getOrElse(s"https://fortismodels.blob.core.windows.net/public/opener-$language.zip")
    if ((!remotePath.startsWith("http://") && !remotePath.startsWith("https://")) || !remotePath.endsWith(".zip")) {
      throw new FileNotFoundException(s"Unable to process $remotePath, should be http(s) link to zip file")
    }
    val localDir = downloadModels(remotePath)
    if (!hasModelFiles(localDir, language)) {
      throw new FileNotFoundException(s"No models for language $language in $remotePath")
    }
    modelDirectories.putIfAbsent(language, localDir)
    localDir
  }

  private def downloadModels(remotePath: String): String = {
    val localFile = Files.createTempFile(getClass.getSimpleName, ".zip").toAbsolutePath.toString
    val localDir = Files.createTempDirectory(getClass.getSimpleName).toAbsolutePath.toString
    logDebug(s"Starting to download models from $remotePath to $localFile")
    val exitCode = (new URL(remotePath) #> new File(localFile)).!
    logDebug(s"Finished downloading models from $remotePath to $localFile")
    if (exitCode != 0) {
      throw new FileNotFoundException(s"Unable to download models from $remotePath")
    }
    new ZipFile(localFile).extractAll(localDir)
    new File(localFile).delete()
    localDir
  }

  private def hasModelFiles(modelsDir: String, language: String): Boolean = {
    if (modelsDir.isEmpty) return false

    val modelFiles = new File(modelsDir).listFiles
    modelFiles != null && modelFiles.exists(_.getName.startsWith(s"$language-"))
  }
}