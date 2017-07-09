package com.rallyhealth.driver

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import reactivemongo.bson.Macros.Options._
import reactivemongo.bson._

sealed trait Box {
  def _id: String
  def length: Int
  def width: Int
  def height: Int
  def manufactureDate: DateTime
  def lastShipped: Option[DateTime]
}

case class CorrugatedBox(
  override val _id: String,
  override val length: Int,
  override val width: Int,
  override val height: Int,
  override val manufactureDate: DateTime,
  override val lastShipped: Option[DateTime],
  layers: Int) extends Box

case class RigidBox(
  override val _id: String,
  override val length: Int,
  override val width: Int,
  override val height: Int,
  override val manufactureDate: DateTime,
  override val lastShipped: Option[DateTime],
  numberOfPiece: Int) extends Box

case class FoldingBox(
  override val _id: String,
  override val length: Int,
  override val width: Int,
  override val height: Int,
  override val manufactureDate: DateTime,
  override val lastShipped: Option[DateTime],
  style: String) extends Box


case class BoxOfBoxes(
  override val _id: String,
  override val length: Int,
  override val width: Int,
  override val height: Int,
  override val manufactureDate: DateTime,
  override val lastShipped: Option[DateTime],
  boxes: Set[Box]) extends Box

object Box {

  implicit object BSONDateTimeHandler extends BSONHandler[BSONDateTime, DateTime] {

    val fmt = ISODateTimeFormat.dateTime()

    def read(time: BSONDateTime) = new DateTime(time.value)

    def write(jdtime: DateTime) = BSONDateTime(jdtime.getMillis)
  }

//  implicit val boxHandler = Macros.handlerOpts[Box, AutomaticMaterialization]
  implicit val corrugatedBoxHandler = Macros.handler[CorrugatedBox]
//  implicit val rigidBoxHandler = Macros.handler[RigidBox]
//  implicit val foldingBoxHandler = Macros.handler[FoldingBox]
//  implicit val boxOfBoxes = Macros.handler[BoxOfBoxes]

}

