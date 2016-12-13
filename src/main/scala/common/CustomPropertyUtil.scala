

package common

import java.io.FileInputStream
import java.util.Properties
import java.io.InputStream
import java.io.File

object CustomPropertyUtil {
  def load(file: File) = {

    var in: InputStream = null
    try {
      in = new FileInputStream(file)
      val props = new Properties()
      props.load(in)

      props

    } finally {
      if (in != null) in.close()
    }
  }

  def load(path: String) = {

    var in: InputStream = null
    try {
      in = CustomPropertyUtil.getClass().getClassLoader().getResourceAsStream(path)
      val props = new Properties()
      props.load(in)

      props

    } finally {
      if (in != null) in.close()
    }
  }
}