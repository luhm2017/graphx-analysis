package utils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by ASUS-PC on 2017/4/19.
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
  implicit class CollectionHelper[K, V](collection: Traversable[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) {
    def reduceByKey(f: (V, V) => V): Traversable[(K, V)] = {
      collection.groupBy(_._1).map { case (_: K, values: Traversable[(K, V)]) => values.reduce((a, b) => (a._1, f(a._2, b._2))) }}

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
