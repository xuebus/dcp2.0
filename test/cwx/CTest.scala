package cwx

import BoDaoCommon.Date.DateTool

object CTest extends App {

  val date = DateTool.daysAgoDate2Day(1)
  println(date)
}
