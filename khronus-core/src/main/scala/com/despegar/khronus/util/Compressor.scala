package com.despegar.khronus.util

import java.nio.ByteBuffer
import java.util.zip.{ Deflater, Inflater }

import net.jpountz.lz4.LZ4Factory

trait Compressor {

  def version: Int

  def compress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer, targetOffset: Int): Int

  def decompress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer)

}

object DeflaterCompressor extends Compressor {

  private val defaultCompressionLevel = -1
  private val encodingCompressedCookieBase: Int = 130

  private val inflatersPool = Pool[Inflater]("inflatersPool", 4, () ⇒ new Inflater(), {
    _.reset()
  })
  private val deflatersPool = Pool[Deflater]("deflatersPool", 4, () ⇒ new Deflater(defaultCompressionLevel), {
    _.reset()
  })

  override def version = encodingCompressedCookieBase

  override def compress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer, targetOffset: Int): Int = {
    val deflater = deflatersPool.take()
    deflater.setInput(inputBuffer.array(), inputOffset, inputLength)
    deflater.finish()
    val targetArray = targetBuffer.array()
    val compressedLength = deflater.deflate(targetArray, targetOffset, targetArray.length - targetOffset)
    deflatersPool.release(deflater)
    compressedLength
  }

  override def decompress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer): Unit = {
    val inflater = inflatersPool.take()

    inflater.setInput(inputBuffer.array(), inputOffset, inputLength)
    inflater.inflate(targetBuffer.array())

    inflatersPool.release(inflater)
  }
}

object LZ4Compressor extends Compressor {
  private val lz4Factory = LZ4Factory.fastestInstance()

  val version: Int = 230

  override def compress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer, targetOffset: Int): Int = {
    lz4Factory.fastCompressor().compress(inputBuffer.array(), inputOffset, inputLength, targetBuffer.array(), targetOffset)
  }

  override def decompress(inputBuffer: ByteBuffer, inputOffset: Int, inputLength: Int, targetBuffer: ByteBuffer): Unit = {
    lz4Factory.fastDecompressor().decompress(inputBuffer.array(), inputOffset, targetBuffer.array(), 0, targetBuffer.array().length)
  }
}