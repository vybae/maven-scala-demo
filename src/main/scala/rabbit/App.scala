package rabbit

/**
 * @author ${user.name}
 */
object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    new LogTopicServer().emit()
//    new LogTopicServer().receive
  }
}
