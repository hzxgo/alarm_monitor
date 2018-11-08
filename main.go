package main

import (
	"os"
	"os/signal"
	"syscall"

	"alarm_monitor/business"
	"alarm_monitor/config"
	"alarm_monitor/model/mysql"
	"alarm_monitor/server"

	"github.com/cihub/seelog"
)

func main() {
	defer destroy()

	s := server.NewServer()
	cfg := config.GetConfig()

	kafka, err := business.NewKafka(cfg.Kafka.Brokers, cfg.Kafka.ReceiveAlarmTopic)
	if err != nil {
		seelog.Errorf("new kafka err: %v", err)
		return
	}
	s.Kafkas = append(s.Kafkas, kafka)

	s.Start()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case n := <-sc:
		seelog.Infof("receive signal %v, closing", n)
	case <-s.Ctx().Done():
		seelog.Infof("context is done with %v, closing", s.Ctx().Err())
	}

	s.Stop()
}

func destroy() {
	mysql.FreeDB()
	seelog.Flush()
}
