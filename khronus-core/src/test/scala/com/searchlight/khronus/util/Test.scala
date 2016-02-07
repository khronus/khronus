package com.searchlight.khronus.util

import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, FunSuite }

trait Test extends FunSuite with Matchers with MockitoSugar {
  import Mockito._
}
