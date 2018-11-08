package model

import (
	"fmt"
	"strconv"
	"time"

	"alarm_monitor/model/mysql"
	"alarm_monitor/model/redis"
)

type AlarmMonitorPolicy struct {
	mysql.Model
}

type AlarmPolicy struct {
	ID              int64
	JobID           int64
	ServiceName     string
	Switch          int
	Policy          int
	Receivers       mysql.NullString
	IntervalMinutes int
}

// ---------------------------------------------------------------------------------------------------------------------

func NewAlarmMonitorPolicy() *AlarmMonitorPolicy {
	return &AlarmMonitorPolicy{
		Model: mysql.Model{
			TableName: TABLE_ALARM_MONITOR_POLICY,
		},
	}
}

func (this *AlarmMonitorPolicy) GetLatestAlarmTime(jobId int64, serviceName string) int64 {
	cacheKey := fmt.Sprintf("%s:%d#%s", RDS_REPORT_ALARM_TIME, jobId, serviceName)
	ret, _ := redis.Get(cacheKey)
	alarmTime, _ := strconv.ParseInt(ret, 10, 64)
	return alarmTime
}

func (this *AlarmMonitorPolicy) SetLatestAlarmTime(jobId int64, serviceName string, timeout int) error {
	cacheKey := fmt.Sprintf("%s:%d#%s", RDS_REPORT_ALARM_TIME, jobId, serviceName)
	_, err := redis.Do("SET", cacheKey, time.Now().Unix(), "EX", timeout)
	return err
}

func (this *AlarmMonitorPolicy) GetSingle(jobId int64, serviceName string) (*AlarmPolicy, error) {
	var ap AlarmPolicy
	ap.JobID = jobId
	ap.ServiceName = serviceName

	// get values from cache
	cacheKey := fmt.Sprintf("%s:%d#%s", RDS_REPORT_ALARM_POLICY, jobId, serviceName)
	ret, _ := redis.Hgetall(cacheKey)
	if len(ret) > 0 {
		if v, ok := ret["switch"]; ok {
			ap.Switch, _ = strconv.Atoi(v)
		}
		if v, ok := ret["policy"]; ok {
			ap.Policy, _ = strconv.Atoi(v)
		}
		if v, ok := ret["receivers"]; ok {
			ap.Receivers.String = v
		}
		if v, ok := ret["interval_minutes"]; ok {
			ap.IntervalMinutes, _ = strconv.Atoi(v)
		}
		return &ap, nil
	}

	// get values from mysql
	exps := map[string]interface{}{
		"job_id=?":       jobId,
		"service_name=?": serviceName,
	}
	query := this.Select("switch, policy, receivers, interval_minutes").Form(this.TableName)
	if row, err := this.SelectWhere(query, exps); err != nil {
		return nil, err
	} else {
		if err = row.Scan(&ap.Switch, &ap.Policy, &ap.Receivers, &ap.IntervalMinutes); err != nil {
			if err.Error() != mysql.ErrNoRows.Error() {
				return nil, err
			}
		}
		mapping := map[string]string{
			"switch":           strconv.Itoa(ap.Switch),
			"policy":           strconv.Itoa(ap.Policy),
			"receivers":        ap.Receivers.String,
			"interval_minutes": strconv.Itoa(ap.IntervalMinutes),
		}
		redis.Hmset(cacheKey, mapping)
		redis.Do("EXPIRE", cacheKey, RDS_EXPIRE_ALARM_POLICY_TIME)
	}

	return &ap, nil
}
