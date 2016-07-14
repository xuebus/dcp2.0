/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序是dcp系统的入口
 * 启动DCPSystem Actor 进行系统初始化
 * 监听、监控系统状态
 */

import java.io.File

import dcp.common.SysStart
import dcp.system.DcpSystem
import play.api._
import play.api.mvc._
import akka.actor.{Props, ActorSystem}

import com.typesafe.config.ConfigFactory

object Global extends GlobalSettings {
  var actorSystem: Option[ActorSystem] = None

  override def onStart(app: Application) {
    val configFile = getClass.getClassLoader.getResource("remote_application.conf").getFile
    //parse the config
    val config = ConfigFactory.parseFile(new File(configFile))
    //create an actor system with that config
    val system = ActorSystem("DcpSystem" , config)
    val dcpSystem = system.actorOf(Props[DcpSystem], name = "DcpSystemActor")

    if (app.configuration.getBoolean("open.backgroup.worker").get) {
      dcpSystem ! SysStart(system)
    }

    println("remote is ready")
    super.onStart(app)
  }

  override def onStop(app: Application) {
    super.onStop(app)
  }

  override def onError(request: RequestHeader, ex: Throwable) = {
    //这里定义自己的出错页面
    super.onError(request, ex)
  }
}