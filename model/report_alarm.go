package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"alarm_monitor/config"
	"alarm_monitor/model/mysql"

	"github.com/cihub/seelog"
)

type ReportAlarm struct {
	mysql.Model
}

// ---------------------------------------------------------------------------------------------------------------------

func NewReportAlarm() *ReportAlarm {
	return &ReportAlarm{
		Model: mysql.Model{
			TableName: TABLE_REPORT_ALARM_PRE + time.Now().Format("200601"),
		},
	}
}

func (this *ReportAlarm) RollingBatchInsert(columns []string, params []interface{}) (int64, error) {
	this.TableName = TABLE_REPORT_ALARM_PRE + time.Now().Format("200601")
	id, err := this.BatchInsert(columns, params)
	if err != nil {
		if strings.HasPrefix(err.Error(), "Error 1146") {
			if err = this.createTable(); err != nil {
				return 0, err
			}
			if id, err = this.BatchInsert(columns, params); err != nil {
				return 0, err
			}
		}

		// delete expired table
		maxRolls := config.GetConfig().Service.MaxStoreMonths
		if err = this.rollTables(maxRolls); err != nil {
			seelog.Errorf("delete expired report_alarm_x table err: %v", err)
		}
	}

	return id, nil
}

func (this *ReportAlarm) RollingInsert(params map[string]interface{}) (int64, error) {
	this.TableName = TABLE_REPORT_ALARM_PRE + time.Now().Format("200601")
	id, err := this.Insert(params)
	if err != nil {
		if strings.HasPrefix(err.Error(), "Error 1146") {
			if err = this.createTable(); err != nil {
				return 0, err
			}
			if id, err = this.Insert(params); err != nil {
				return 0, err
			}
		}

		// delete expired table
		maxRolls := config.GetConfig().Service.MaxStoreMonths
		if err = this.rollTables(maxRolls); err != nil {
			seelog.Errorf("delete expired report_alarm_x table err: %v", err)
		}
	}

	return id, nil
}

// ---------------------------------------------------------------------------------------------------------------------

func (this *ReportAlarm) createTable() error {
	this.TableName = TABLE_REPORT_ALARM_PRE + time.Now().Format("200601")
	cmd := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` ( "+
		"`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'id', "+
		"`job_id` bigint(20) DEFAULT '0' COMMENT '服务ID', "+
		"`service_name` varchar(127) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '服务名称', "+
		"`content` text CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '报警内容', "+
		"`create_time` bigint(20) DEFAULT '0' COMMENT '创建时间戳', "+
		"`is_alarm` tinyint(1) DEFAULT '0' COMMENT '报警标识：0.未报警、1.已报警', "+
		"`not_alarm_reason` text CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '未报警原因', "+
		"`alarm_policy` tinyint(1) DEFAULT '0' COMMENT '报警策略：0.钉钉机器人报警', "+
		"`receivers` text CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '接收者', "+
		"PRIMARY KEY (`id`), "+
		"KEY `idx_job_id` (`job_id`) USING BTREE, "+
		"KEY `idx_service_name` (`service_name`) USING BTREE, "+
		"KEY `idx_is_alarm` (`is_alarm`) USING BTREE "+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;", this.TableName)

	_, err := this.GetDB().Query(cmd)
	if err != nil {
		return err
	}

	return nil
}

func (this *ReportAlarm) rollTables(maxRolls uint32) error {
	if maxRolls == 0 {
		return nil
	}

	cmd := fmt.Sprintf("SHOW TABLES LIKE '%s%s'", TABLE_REPORT_ALARM_PRE, "%")
	threshold, err := strconv.Atoi(time.Now().AddDate(0, -int(maxRolls), 0).Format("200601"))
	if err != nil {
		return err
	}

	if rows, err := this.GetDB().Query(cmd); err != nil {
		return err
	} else {
		for rows.Next() {
			var table string
			if err = rows.Scan(&table); err != nil {
				return err
			}

			month, err := strconv.Atoi(table[len(TABLE_REPORT_ALARM_PRE):])
			if err == nil && month < threshold {
				if _, err = this.GetDB().Query("DROP TABLE " + table); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
