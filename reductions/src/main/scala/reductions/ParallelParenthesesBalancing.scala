package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {

    val func = (acc: Int, y : Char) => if (y == '(' ) acc + 1 else if (y == ')' ) acc - 1 else acc
    //val func = (x: Int, y: Char) => x + 1

    val acc = chars.foldLeft(0)((x,y) => func(x,y))
    println("acc: "+acc )
    acc == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int) : (Int,Int) = {
      val newArray = chars.slice(idx,until)
      val func = (a : (Int,Int), y: Char) => if (y == '(' ) (a._1 + 1 , a._2) else if (y == ')' ) (a._1, a._2 + 1) else a
      val tuple = newArray.foldLeft((0,0))((x,y) => func(x,y))
      println("tuple: " + tuple)
      tuple
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      val size = until - from
      if(size <= threshold) traverse(from,until,0,0)
      else {
        val middle = (until + from) / 2
        val (left,right) = parallel(reduce(from,middle) , reduce(middle, until) )
         val matches = scala.math.min(left._1, right._2)
         (left._1 + right._1 - matches , left._2 + right._2 - matches )
      
      }
    }

    reduce(0, chars.length) == (0,0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
