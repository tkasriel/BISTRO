package beam.competition.inputs.impl

object SiouxFauxConstants {

  val ROUTE_NUMBER: List[Int] = List(1340, 1341, 1342, 1343, 1344, 1345, 1346, 1347, 1348, 1349, 1350, 1351)

  val AGE_START: List[Int] = List(1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 81, 86, 91, 96, 101, 106, 111, 116)
  val AGE_END: List[Int] = List(5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 115, 120)

  val INCOME_START: List[Int] = List(0, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 55000,
    60000, 65000, 70000, 75000, 80000, 85000, 90000, 95000, 100000, 105000, 110000,
    115000, 120000, 125000, 130000, 135000, 140000, 145000)
  val INCOME_END: List[Int] = List(4999, 9999, 14999, 19999, 24999, 29999, 34999, 39999, 44999, 49999, 54999, 59999, 64999, 69999, 74999,
    79999, 84999, 89999, 94999, 99999, 104999, 109999, 114999, 119999, 124999, 129999, 134999, 139999, 144999,
    150000)

  val BUS_SCHEDULE_START_TIMES: List[Int] = List(21600, 27001, 36001, 50401, 57601, 68401)

  val BUS_SCHEDULE_END_TIMES: List[Int] = List(27000, 36000, 50400, 57600, 68400, 79200)

  val AVAILABLE_HEADWAY: List[Int] = List(180, 360, 540, 720, 900, 1080, 1260, 1440, 1620, 1800, 1980, 2160, 2340, 2520, 2700, 2880, 3060,
    3240, 3420, 3600, 3780, 3960, 4140, 4320, 4500, 4680, 4860, 5040, 5220, 5400, 5580, 5760, 5940,
    6120, 6300, 6480, 6660, 6840, 7020)

  val INCENTIVIZED_MODE: List[String] = List("walk_transit", "drive_transit", "OnDemand_ride")
}
