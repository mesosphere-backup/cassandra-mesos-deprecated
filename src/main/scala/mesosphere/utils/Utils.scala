package mesosphere.utils

import org.apache.mesos.Protos.TaskID

object Slug {
  def apply(input: String) = slugify(input)

  def slugify(input: String): String = {
    import java.text.Normalizer
    Normalizer.normalize(input, Normalizer.Form.NFD)
      .replaceAll("[^\\w\\s-]", "") // Remove all non-word, non-space or non-dash characters
      .replace('-', ' ')            // Replace dashes with spaces
      .trim                         // Trim leading/trailing whitespace (including what used to be leading/trailing dashes)
      .replaceAll("\\s+", "-")      // Replace whitespace (including newlines and repetitions) with single dashes
      .toLowerCase                  // Lowercase the final results
  }
}

object TaskIDUtil {

  val taskDelimiter = "_"

  def taskId(appName: String, sequence: Int) = {
    "%s%s%d-%d".format(appName, taskDelimiter, sequence, System.currentTimeMillis())
  }

  def appID(taskId: TaskID) = {
    val taskIdString = taskId.getValue
    taskIdString.substring(0, taskIdString.lastIndexOf(taskDelimiter))
  }
}
