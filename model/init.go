package model

import (
	"os"

	"alarm_monitor/config"
	"alarm_monitor/model/mysql"

	"github.com/cihub/seelog"
)

func init() {
	cfg := config.GetConfig()

	// init mysql
	if err := mysql.Init(cfg.Mysql.DataSource); err != nil {
		seelog.Criticalf("init mysql err %v", err)
		os.Exit(1)
		return
	}

	seelog.Infof("--------------------------------------------------")
	seelog.Infof("Service Name: %s", service_name)
	seelog.Infof("Service Version: %s", service_version)
	seelog.Infof("Service Release: %s", source_latest_push)
	seelog.Infof("--------------------------------------------------")
}
