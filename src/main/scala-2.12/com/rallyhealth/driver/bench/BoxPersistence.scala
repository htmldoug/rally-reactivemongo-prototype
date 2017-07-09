package com.rallyhealth.driver.bench

import com.rallyhealth.driver.CorrugatedBox

import scala.concurrent.Future

trait BoxPersistence[SaveResult, DeleteResult] {

  def save(box: CorrugatedBox): Future[SaveResult]

  def findCorrugatedBoxById(id: String): Future[Option[CorrugatedBox]]

  def deleteAll(): Future[DeleteResult]
}
