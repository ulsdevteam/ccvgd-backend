class MESSAGE:
  OK = "0"
  DBERR = "4001"
  NODATA = "4002"
  DATAEXIST = "4003"
  DATAERR = "4004"
  SESSIONERR = "4101"
  LOGINERR = "4102"
  PARAMERR = "4103"
  USERERR = "4104"
  ROLEERR = "4105"
  PWDERR = "4106"
  REQERR = "4201"
  IPERR = "4202"
  THIRDERR = "4301"
  IOERR = "4302"
  SERVERERR = "4500"
  UNKOWNERR = "4501"


ret_map = {
  MESSAGE.OK: u"Success",
  MESSAGE.DBERR: u"Database Error",
  MESSAGE.NODATA: u"No data",
  MESSAGE.DATAEXIST: u"Data already exist",
  MESSAGE.DATAERR: u"Data Error",
  MESSAGE.SESSIONERR: u"User not logged in",
  MESSAGE.LOGINERR: u"User login failed",
  MESSAGE.PARAMERR: u"Parameter error",
  MESSAGE.USERERR: u"The user does not exist or is not activated",
  MESSAGE.ROLEERR: u"User identity error",
  MESSAGE.PWDERR: u"Wrong password",
  MESSAGE.REQERR: u"Illegal request or limited number of requests",
  MESSAGE.IPERR: u"IP restricted",
  MESSAGE.THIRDERR: u"Third party system error",
  MESSAGE.IOERR: u"File read / write error",
  MESSAGE.SERVERERR: u"Internal error",
  MESSAGE.UNKOWNERR: u"Unknown error",
}
