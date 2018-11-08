package config

import (
	"os"
	"strings"

	"github.com/cihub/seelog"
)

func init() {
	var configFile string
	env := strings.ToUpper(os.Getenv("ENV"))
	switch env {
	case "DEV":
		configFile = "./conf.d/dev_log.xml"
	case "TEST":
		configFile = "./conf.d/test_log.xml"
	case "BETA":
		configFile = "./conf.d/beta_log.xml"
	case "PRODUCT":
		configFile = "./conf.d/product_log.xml"
	default:
		configFile = "./conf.d/dev_log.xml"
	}

	logger, err := seelog.LoggerFromConfigAsFile(configFile)
	if err != nil {
		seelog.Critical("parse log config err: %v", err)
		os.Exit(0)
		return
	}

	seelog.ReplaceLogger(logger)
}
