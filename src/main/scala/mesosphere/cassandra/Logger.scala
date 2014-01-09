package mesosphere.cassandra

import org.apache.log4j.{Priority, Level, Logger}

/**
 * A thin wrapper around log4j.
 *
 */

trait Logger {

  lazy val logger = Logger.getLogger(getClass)

  def getRootLogger = Logger.getRootLogger()

  def isTraceEnabled = logger.isTraceEnabled

  def trace(msg: => AnyRef) = if (isTraceEnabled) logger.trace(msg)

  def trace(msg: => AnyRef, t: => Throwable) = if (isTraceEnabled) logger.trace(msg, t)

  def assertLog(assertion: Boolean, msg: => String) = if (assertion) logger.assertLog(assertion, msg)

  def isDebugEnabled = logger.isDebugEnabled

  def debug(msg: => AnyRef) = if (isDebugEnabled) logger.debug(msg)

  def debug(msg: => AnyRef, t: => Throwable) = if (isDebugEnabled) logger.debug(msg, t)

  def isErrorEnabled = logger.isEnabledFor(Level.ERROR)

  def error(msg: => AnyRef) = if (isErrorEnabled) logger.error(msg)

  def error(msg: => AnyRef, t: => Throwable) = if (isErrorEnabled) logger.error(msg, t)

  def fatal(msg: AnyRef) = logger.fatal(msg)

  def fatal(msg: AnyRef, t: Throwable) = logger.fatal(msg, t)

  def level = logger.getLevel()

  def isEnabledFor(level: Level) = logger.isEnabledFor(level)

  def name = logger.getName

  def isInfoEnabled = logger.isInfoEnabled

  def info(msg: => AnyRef) = if (isInfoEnabled) logger.info(msg)

  def info(msg: => AnyRef, t: => Throwable) = if (isInfoEnabled) logger.info(msg, t)

  def isEnabledFor(level: Priority) = logger.isEnabledFor(level)

  def isWarnEnabled = isEnabledFor(Level.WARN)

  def warn(msg: => AnyRef) = if (isWarnEnabled) logger.warn(msg)

  def warn(msg: => AnyRef, t: => Throwable) = if (isWarnEnabled) logger.warn(msg, t)
}
