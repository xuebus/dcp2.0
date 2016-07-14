/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供上传数据到hbase的功能
 * 现在程序提供将usertrack上传到WebTrackData数据表
 */

package dcp.business.db

import hapi.HBColumn.HBRowCell
import hapi.HWriter
import hapi.Puter.HBFamilyPuter

import dcp.business.service.TrackInHbase

object HbaseStorager {
  private val optionFilter: (Option[Any], Option[Any] => Boolean) => Boolean =
    (opt: Option[Any], func: Option[Any] => Boolean) => {
      func(opt)
    }
  private def trackInHbaseOptFilter(opt: Option[Any]): Boolean = {
    opt match {
      case None => false
      case Some(t) => true
    }
  }

  private def setRowCell(rowCell: HBRowCell, trackInHbase: TrackInHbase) = {
    trackInHbase.actions match {
      case None =>
      case Some(actions) => rowCell.addRowCell("Action", actions)
    }
    rowCell.addRowCell("Duration", trackInHbase.totalDur)
    rowCell.addRowCell("PageInfos", trackInHbase.pageInfos.get) //判空操作在格式化的时候已经进行过了
    rowCell.addRowCell("Referer", trackInHbase.ref)
    rowCell.addRowCell("VisitTime", trackInHbase.browseTime)
    rowCell.addRowCell("Mid", trackInHbase.mid)
  }

  def putTracks2Hbase(trackOpts: TraversableOnce[Option[TrackInHbase]],
                      puter: HBFamilyPuter, hbWriter: HWriter): Boolean = {
    //将None的数据过滤掉
    val rows = trackOpts filter { elem => this.optionFilter(elem, this.trackInHbaseOptFilter) } map { trackOpt =>
      val trackInHbase = trackOpt.get
      val rowCell = new HBRowCell(trackInHbase.uid)
      this.setRowCell(rowCell, trackInHbase)
      rowCell
    }

    if (rows.nonEmpty) {
      puter.data = rows
      hbWriter.putRowsAtFamily(puter)
    } else {  //空的时候直接返回
      true
    }
  }
}