object StopWords {
  // We filter for "", "'", and "_", which can 'leak'
  // through our simple inverted index tokenization.
  val words = Set("", "'", "_", "a", "al", "ante", "antecabe", "aquella", "aquello", "aquí", "allí", "allá", "b", "c",
    "cabe", "cada", "como", "cómo", "con", "contra", "cual", "cuál", "cuando", "cuándo", "cuanto", "cuánto", "d", "de",
    "del","decir", "dicen", "dicho", "desde", "después", "donde", "dónde", "durante", "e", "el", "él", "en", "entre",
    "es", "eso", "esto", "esa", "esta", "estar", "estoy", "ella", "ello", "ellos", "era", "estaba", "f", "fue", "g",
    "h", "haber", "ha", "he", "había", "hacer", "hago", "hacía", "hizo", "han", "has", "hacia", "hasta", "hay", "haz",
    "hubo", "i", "ir", "ido", "j", "k", "l", "la", "lo", "los", "las", "m", "mi", "mío", "más", "mas", "menos", "mismo",
    "misma", "mis", "mucho", "mucha", "muchos", "muchas", "muchísimo", "muchísimos", "muchísima", "muchísimas", "n",
    "no", "nosotros", "nuestro", "nos", "o", "os", "otro", "otra", "otros", "otras", "p", "para", "pero", "por", "q",
    "que", "qué", "quien", "quién", "r", "s", "sino", "suyo", "su", "ser", "se", "si", "sido", "sigo", "sus", "soy",
    "t", "tanto", "tanta", "tantos", "tantas", "tener", "tengo", "tenía", "todo", "todos", "toda", "todas", "tú", "tu",
    "tuyo", "tuya", "u", "un", "uno", "una", "unos", "unas", "v", "vosotros", "vosotras", "vuestro", "vuestra", "w",
    "x", "y", "ya", "yo", "z")
}