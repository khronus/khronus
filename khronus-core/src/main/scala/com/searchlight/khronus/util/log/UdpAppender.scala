/*
 * =========================================================================================
 * Copyright © 2015 the khronus project <https://github.com/hotels-tech/khronus>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package com.searchlight.khronus.util.log

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
    val buf = encoder.getLayout.doLayout(event).trim().getBytes
    try {
      val address: InetAddress = InetAddress.getByName(ip)
      val packet: DatagramPacket = new DatagramPacket(buf, buf.length, address, port)
      socket.send(packet)
    } catch {
      case e: Exception ⇒ e.printStackTrace()
    }

  }

  override def stop(): Unit = {
    if (!socket.isClosed) {
      socket.close()
    }
    super.stop()
  }
}