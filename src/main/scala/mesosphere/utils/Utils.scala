package mesosphere.utils

object Slug {
  def apply(input:String) = slugify(input)

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
