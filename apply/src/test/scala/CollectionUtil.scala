import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by liuchen on 2017/8/10.
  * Description:
  */
object CollectionUtil {

  /**
    * 对具有Traversable[(K, V)]类型的集合添加reduceByKey相关方法
    *
    * @param collection
    * @param kt
    * @param vt
    * @tparam K
    * @tparam V
    */
  implicit class CollectionHelper[K, V](collection: ArrayBuffer[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) {
    def reduceByKeyMy(f: (V, V) => V): Traversable[(K, V)] = {
      val group: Map[K, ArrayBuffer[(K, V)]] = collection.groupBy(_._1)
      group.map(x => x._2.reduce((a, b) => (a._1, f(a._2, b._2))))
    }


    /**
      * reduceByKey的同时，返回被reduce掉的元素的集合
      *
      * @param f
      * @return
      */
    def reduceByKeyWithReduced(f: (V, V) => V)(implicit kt: ClassTag[K], vt: ClassTag[V]): (Traversable[(K, V)], Traversable[(K, V)]) = {
      val reduced: ArrayBuffer[(K, V)] = ArrayBuffer()
      val newSeq = collection.groupBy(_._1).map {
        case (_: K, values: Traversable[(K, V)]) => values.reduce((a, b) => {
          val newValue: V = f(a._2, b._2)
          val reducedValue: V = if (newValue == a._2) b._2 else a._2
          val reducedPair: (K, V) = (a._1, reducedValue)
          reduced += reducedPair
          (a._1, newValue)
        })
      }
      (newSeq, reduced.toTraversable)
    }
  }

}
