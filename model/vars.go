package model

const (
	service_name       = "alarm_monitor_center"
	service_version    = "V0.0.1"
	source_latest_push = "2018-11-06"
)

// table name
const (
	TABLE_REPORT_ALARM_PRE     = "report_alarm_"        // 上报警告表前缀
	TABLE_ALARM_MONITOR_POLICY = "alarm_monitor_policy" // 报警接听策略
)

// redis key
const (
	RDS_REPORT_ALARM_POLICY = "monitor:alarm:policy" // 监控警告策略
	RDS_REPORT_ALARM_TIME   = "monitor:alarm:time"   // 报警时间
)

// alarm policy
const (
	ALARM_POLICY_DINGTALK_ROBOT = 0 // 报警策略：钉钉机器人报警
)

// others
const (
	REPORT_ALARM_FILTERED_TIME   = 3600 // 1 小时
	CHAN_CONSUMER_MSG_CAPS       = 100  // 消费者的消息通道容量
	BATCH_INSERT_CAPS            = 200  // 批量插入最大容量
	BATCH_INSERT_INTERVAL_TIME   = 90   // 90秒
	RDS_EXPIRE_ALARM_POLICY_TIME = 600  // 10分钟
)
