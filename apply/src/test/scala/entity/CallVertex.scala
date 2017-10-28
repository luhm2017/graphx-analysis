package entity

import scala.reflect.ClassTag

/**
  * Created by ASUS-PC on 2017/4/20.
  */
case class CallVertex[VD: ClassTag](var oldAttr: VD = null,
                                    var newAttr: VD = null,
                                    var init: Boolean = false,
                                    var loop: Int = 0)
  extends Serializable {
}
