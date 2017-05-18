package database.sequence

trait Sequence[V] {
  def nextVal(): V
  def nextVals(n: Int): Iterable[V]
  def curVal(): V
  def setVal(value: V): Unit
  def force(): Unit
}