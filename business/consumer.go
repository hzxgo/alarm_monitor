package business

type Consumer interface {
	Start() error
	Stop() error
}

type ReceiverAlarmMsg struct {
	JobID          int64  `json:"job_id"`
	ServiceName    string `json:"service_name"`
	Content        string `json:"content"`
	HeartTime      int64  `json:"heart_time"`
	IsAlarm        int    `json:"-"`
	NotAlarmReason string `json:"-"`
	AlarmTime      int64  `json:"-"`
	AlarmPolicy    int    `json:"-"`
	Receivers      string `json:"-"`
}
