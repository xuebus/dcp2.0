/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供创建hbase数据库的功能
 * 对当前系统dcp来说, 需要${商家名字}_WebTrackData 这张hbase数据库表
 * 这张hbase数据表主要用来保存已经清洗过的用户轨迹,这个表只有一个列簇, 这个列簇包括:
 * key: 用户id, 不会为空
 * Action: 轨迹类型的次数统计(json格式)，类型分为v，f， s, 不会为空
 * Duration: 用户在系统中停留的总时间, 不会为空
 * PageInfos: 用户在系统中浏览的每个页面的信息(json格式), 包括每个页面的tags, 和dur(停留时间), 不会为空
 * Reffer: 用户进入系统前的reffer, 代表着用户的来源, 不会为空(如果没有, 则是系统配置的默认值)
 * VistTime: 用户进入系统的时间, 不会为空
 */

package dcp.business.db

import org.apache.hadoop.hbase.client.Admin

import dcp.config.DynConfiguration
import hapi.{HManager, HFamily}

/**
  * hbase 数据表的family的属性设置
  * @param familyName　表的family
  * @param attrs 表的属性
  */
case class HBFamilyWithAttr(familyName: String, attrs: Map[String, String])

/**
  * 提供创建商家数据库的API
  */
object HbaseDB {
  private val _busiName = this.businessName
  private val familiesAttrConf = this.familiesAttrConfig()

  private def familiesAttrConfig(): Map[String, String] = {
    import BoDaoCommon.File.LocalFileTool
    val cfgFile = DynConfiguration.getStringOrElse("hbase.familiesconfig.file", "./DynConfig/HFamiliesArrt.conf")
    val cfgOpt = LocalFileTool.readFile2kv(cfgFile, "=", '#')
    cfgOpt match {
      case None => throw new Exception(s"can't load $cfgFile")
      case Some(cfg) => cfg
    }
  }

  private def webtrackDBName = DynConfiguration.getStringOrElse("hbase.table.webtrack", "WebTrackData")

  private def businessName = DynConfiguration.getStringOrElse("business.name", "")

  private def createHTable(admin: Admin, tableName: String,
                           familiesAttr: TraversableOnce[HBFamilyWithAttr], force: Boolean = false): Boolean = {
    val families = familiesAttr map { familyWithAttr =>
      val familiy = new HFamily(familyWithAttr.familyName)
      familiy.setHColumnDescriptorAttrs(familyWithAttr.attrs)
      familiy
    }

    force match {
      case false => HManager.createTable(admin, tableName, families)
      case true => HManager.createTableForce(admin, tableName, families)
    }
  }

  /**
    * 当前dcp系统只使用Tracks这个列簇
    * @param admin hbase 管理者
    * @param force 是否把原来存在的表删除, 这个操作非常危险
    * @return
    */
  private def createWebTrackDB(admin: Admin, force: Boolean = false): Boolean = {
    val webTrackTableName = s"${this._busiName}_${this.webtrackDBName}"
    val familiesAttr = Array(HBFamilyWithAttr("Tracks", this.familiesAttrConf))
    this.createHTable(admin, webTrackTableName, familiesAttr, force)
  }

  def createHbaseDB(admin: Admin, force: Boolean = false): Boolean = {
    if (!this.createWebTrackDB(admin, force)) return false
    true
  }
}