package com.despegar.metrik.util.log

import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketException
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import scala.beans.BeanProperty

/**
 * Appender that send data via UDP
 */
class UpdAppender extends AppenderBase[ILoggingEvent] {

  @BeanProperty
  var encoder: PatternLayoutEncoder = _

  @BeanProperty
  var port: Int = 0

  @BeanProperty
  var ip: String = _

  var socket: DatagramSocket = _

  override def start(): Unit = {
    if (this.encoder == null) {
      addError("No layout of udp appender")
      return
    }
    if (socket == null) {
      try {
        socket = new DatagramSocket()
      } catch {
        case e: SocketException ⇒ e.printStackTrace()
      }
    }
    super.start()
  }

  override protected def append(event: ILoggingEvent): Unit = {
    val buf = encoder.getLayout().doLayout(event).trim().getBytes()
    try {
      val address: InetAddress = InetAddress.getByName(ip)
      val packet: DatagramPacket = new DatagramPacket(buf, buf.length, address, port)
      socket.send(packet)
    } catch {
      case e: Exception ⇒ e.printStackTrace()
    }

  }

  override def stop(): Unit = {
    if (!socket.isClosed()) {
      socket.close()
    }
    super.stop()
  }
}