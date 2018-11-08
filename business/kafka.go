package business

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"alarm_monitor/config"
	"alarm_monitor/model"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/cihub/seelog"
)

type Kafka struct {
	l                       sync.Mutex                      // 锁
	consumerGroup           *cluster.Consumer               // 消费者
	partitionConsumers      []sarama.PartitionConsumer      // 指定分区消费者
	partitionOffsetManagers []sarama.PartitionOffsetManager // 维护Offset
	receiveAllPartition     bool                            // 接收所以分区消息
	chanConsumerMsg         chan *sarama.ConsumerMessage    // 消费消息通道
	chanExit                chan struct{}                   // 携程退出消息通道
	reportAlarmModel        *model.ReportAlarm              // 上报状态模型
	monitorPolicyModel      *model.AlarmMonitorPolicy       // 监控策略模型
}

type cache struct {
	l                 sync.Mutex    // 锁
	firstMsgTimestamp int64         // 第一条消息存入的时间戳
	values            []interface{} // 批量操作的对象值
}

var (
	msgCache *cache // 消费的消息不直接写MySQL，而是写入缓存，然后批量写入MySQL
)

func init() {
	msgCache = &cache{
		values: make([]interface{}, 0, model.BATCH_INSERT_CAPS),
	}
}

// ---------------------------------------------------------------------------------------------------------------------

func NewKafka(brokers []string, topic string) (*Kafka, error) {
	var err error
	var consumerGroup *cluster.Consumer
	var partitionConsumers []sarama.PartitionConsumer
	var partitionOffsetManagers []sarama.PartitionOffsetManager

	cfg := config.GetConfig()
	groupId := "alarm_monitor_center"

	// create kafka consumer
	if cfg.Kafka.ReceivePartitions.IsAll {
		consumerConfig := cluster.NewConfig()
		consumerConfig.Consumer.Return.Errors = true
		consumerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
		consumerGroup, err = cluster.NewConsumer(brokers, groupId, []string{topic}, consumerConfig)
		if err != nil {
			return nil, err
		}
	} else {
		consumerConfig := sarama.NewConfig()
		consumerConfig.Consumer.Return.Errors = true
		consumerConfig.Version = sarama.V0_11_0_2
		consumerConfig.Consumer.Offsets.CommitInterval = 1 * time.Second
		partitionNum := len(cfg.Kafka.ReceivePartitions.Partitions)
		partitionConsumers = make([]sarama.PartitionConsumer, 0, partitionNum)
		partitionOffsetManagers = make([]sarama.PartitionOffsetManager, 0, partitionNum)

		client, err := sarama.NewClient(brokers, consumerConfig)
		if err != nil {
			return nil, err
		}

		consumer, err := sarama.NewConsumer(brokers, consumerConfig)
		if err != nil {
			return nil, err
		}

		offsetManager, err := sarama.NewOffsetManagerFromClient(groupId, client)
		if err != nil {
			return nil, err
		}

		// 为每个 partition 创建一个消费者并创建一个 Offset 偏移量的管理者
		for _, partition := range cfg.Kafka.ReceivePartitions.Partitions {
			partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
			if err != nil {
				return nil, err
			}

			// 仅在第一次创建 partition 的消费者时才请求已消费消息的偏移量，后期通过偏移量管理者自己维护偏移量
			nextOffset, _ := partitionOffsetManager.NextOffset()
			partitionConsumer, err := consumer.ConsumePartition(topic, partition, nextOffset)
			if err != nil {
				return nil, err
			}
			partitionConsumers = append(partitionConsumers, partitionConsumer)
			partitionOffsetManagers = append(partitionOffsetManagers, partitionOffsetManager)
		}
	}

	return &Kafka{
		consumerGroup:           consumerGroup,
		partitionConsumers:      partitionConsumers,
		partitionOffsetManagers: partitionOffsetManagers,
		receiveAllPartition:     cfg.Kafka.ReceivePartitions.IsAll,
		reportAlarmModel:        model.NewReportAlarm(),
		monitorPolicyModel:      model.NewAlarmMonitorPolicy(),
		chanExit:                make(chan struct{}),
		chanConsumerMsg:         make(chan *sarama.ConsumerMessage, model.CHAN_CONSUMER_MSG_CAPS),
	}, nil
}

// 开启接收数据
func (this *Kafka) Start() error {

	// receiver msg from kafka
	go this.receiver()

	// consumer msg
	go this.consumerMsg()

	return nil
}

// 释放资源
func (this *Kafka) Stop() error {
	this.l.Lock()
	defer this.l.Unlock()

	close(this.chanExit)
	close(this.chanConsumerMsg)

	if this.receiveAllPartition {
		if err := this.consumerGroup.Close(); err != nil {
			seelog.Errorf("close kafka consumer err: %v", err)
		}
	} else {
		for _, v := range this.partitionConsumers {
			if err := v.Close(); err != nil {
				seelog.Errorf("close kafka partition consumer err: %v", err)
			}
		}
		for _, v := range this.partitionOffsetManagers {
			if err := v.Close(); err != nil {
				seelog.Errorf("close kafka partition offset manager err: %v", err)
			}
		}
	}

	// 将消息缓存清空输出至 MySQL
	if err := this.flushMsgCache(); err != nil {
		seelog.Errorf("flush msg cache to mysql error: %v", err)
	}

	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

func (this *Kafka) receiver() {
	var err error
	var msg *sarama.ConsumerMessage

	if this.receiveAllPartition {

		// 消费者接收所有 partition 的消息
		for {
			select {
			case <-this.chanExit:
				return
			case err = <-this.consumerGroup.Errors():
				if err != nil {
					seelog.Errorf("consumer receiver err: %v", err)
				}
			case msg = <-this.consumerGroup.Messages():
				this.chanConsumerMsg <- msg
				this.consumerGroup.MarkOffset(msg, "")
			}
		}
	} else {

		// 为每个 partition 创建一个消费者
		for i, partitionConsumer := range this.partitionConsumers {
			go func(j int, pc sarama.PartitionConsumer) {
				length := len(this.partitionOffsetManagers)
				for {
					select {
					case <-this.chanExit:
						return
					case err = <-pc.Errors():
						if err != nil {
							seelog.Errorf("consumer receiver err: %v", err)
						}
					case msg = <-pc.Messages():
						this.chanConsumerMsg <- msg

						// 消息已消费，手动标记偏移量
						if j < length && this.partitionOffsetManagers[j] != nil {
							nextOffset, _ := this.partitionOffsetManagers[j].NextOffset()
							this.partitionOffsetManagers[j].MarkOffset(nextOffset+1, "")
						}
					}
				}
			}(i, partitionConsumer)
		}
	}
}

func (this *Kafka) consumerMsg() {
	var err error
	var msg *sarama.ConsumerMessage
	var isNotClosed bool

	for {
		select {
		case msg, isNotClosed = <-this.chanConsumerMsg:
			if !isNotClosed {
				return
			}

			var alarmObj ReceiverAlarmMsg
			if err = json.Unmarshal(msg.Value, &alarmObj); err != nil {
				seelog.Errorf("json unmarshal failed. [T:%s P:%d O:%d M:%s], err: %v",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Value), err)
				continue
			}
			if time.Now().Unix()-alarmObj.HeartTime < model.REPORT_ALARM_FILTERED_TIME {
				this.alarm(msg, &alarmObj)
			} else {
				alarmObj.NotAlarmReason = "heart_time expired"
				seelog.Debugf("heart_time expired, will not alarm. [T:%s P:%d O:%d M:%s]",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
			}
			if err = this.store(&alarmObj); err != nil {
				seelog.Errorf("insert alarm err. [T:%s P:%d O:%d M:%s], err: %v",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Value), err)
				continue
			}
		}
	}
}

func (this *Kafka) store(alarmObj *ReceiverAlarmMsg) error {
	if alarmObj == nil || alarmObj.ServiceName == "" {
		return errors.New("params error, alarmObj is null or serviceName is empty")
	}

	columns := []string{
		"job_id", "service_name", "content", "create_time",
		"is_alarm", "not_alarm_reason", "alarm_policy", "receivers",
	}
	value := []interface{}{
		alarmObj.JobID, alarmObj.ServiceName, alarmObj.Content, time.Now().Unix(),
		alarmObj.IsAlarm, alarmObj.NotAlarmReason, alarmObj.AlarmPolicy, alarmObj.Receivers,
	}

	msgCache.l.Lock()
	defer msgCache.l.Unlock()

	length := len(msgCache.values)
	diffTime := time.Now().Unix() - msgCache.firstMsgTimestamp - model.BATCH_INSERT_INTERVAL_TIME

	if length >= model.BATCH_INSERT_CAPS {
		seelog.Warnf("000 batch caps is full. length: %d", length)

		_, err := this.reportAlarmModel.RollingBatchInsert(columns, msgCache.values)
		if err != nil {
			return err
		}

		// 复位 msgCache
		this.resetMsgCache()

		// 将消息写入缓存并记录时间
		msgCache.values = append(msgCache.values, value)
		if msgCache.firstMsgTimestamp == 0 {
			msgCache.firstMsgTimestamp = time.Now().Unix()
		}
	} else if diffTime > 0 && msgCache.firstMsgTimestamp > 0 {
		msgCache.values = append(msgCache.values, value)
		_, err := this.reportAlarmModel.RollingBatchInsert(columns, msgCache.values)
		if err != nil {
			return err
		}

		// 复位 msgCache
		this.resetMsgCache()
	} else {
		msgCache.values = append(msgCache.values, value)
		if msgCache.firstMsgTimestamp == 0 {
			msgCache.firstMsgTimestamp = time.Now().Unix()
		}
	}

	return nil
}

// 将缓存中的数据写入到 MySQL（系统退出前执行）
func (this *Kafka) flushMsgCache() error {
	msgCache.l.Lock()
	defer msgCache.l.Unlock()

	columns := []string{
		"job_id", "service_name", "content", "create_time",
		"is_alarm", "not_alarm_reason", "alarm_policy", "receivers",
	}

	length := len(msgCache.values)
	if length > 0 {
		_, err := this.reportAlarmModel.RollingBatchInsert(columns, msgCache.values)
		if err != nil {
			return err
		}

		// 复位 msgCache
		this.resetMsgCache()
	}

	return nil
}

// 复位消息缓存
func (this *Kafka) resetMsgCache() {
	msgCache.firstMsgTimestamp = 0
	msgCache.values = msgCache.values[0:0]
}

// 报警处理
func (this *Kafka) alarm(msg *sarama.ConsumerMessage, alarmObj *ReceiverAlarmMsg) error {
	if msg == nil || alarmObj == nil || alarmObj.ServiceName == "" {
		return errors.New("params error, alarm object is nil or service_name is empty")
	}

	apm, err := this.monitorPolicyModel.GetSingle(alarmObj.JobID, alarmObj.ServiceName)
	if err != nil {
		alarmObj.NotAlarmReason = err.Error()
		return err
	}
	if apm.Switch == 0 || apm.Receivers.String == "" {
		alarmObj.NotAlarmReason = "switch is closed or receivers is empty"
		seelog.Debugf("alarm switch is closed or receivers is empty. [T:%s P:%d O:%d M:%s]",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		return nil
	}
	if apm.IntervalMinutes < 10 {
		apm.IntervalMinutes = 10
	}

	alarmTime := this.monitorPolicyModel.GetLatestAlarmTime(alarmObj.JobID, alarmObj.ServiceName)
	balance := int64(apm.IntervalMinutes*60) + alarmTime - time.Now().Unix()
	if balance > 0 {
		alarmObj.NotAlarmReason = "alarm too frequent"
		seelog.Debugf("filtered. alarm too frequent. [BT:%d T:%s P:%d O:%d M:%s]",
			balance, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		return nil
	}

	var isOk bool
	switch apm.Policy {
	case model.ALARM_POLICY_DINGTALK_ROBOT:
		isOk, err = this.alarmByDingTalkRobot(alarmObj.Content, msg, apm)
		if !isOk && err != nil {
			alarmObj.NotAlarmReason = err.Error()
			seelog.Errorf("ding talk alarm error, [T:%s P:%d O:%d M:%s] err: %v",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Value), err)
		}
	default:
	}

	alarmObj.AlarmPolicy = apm.Policy

	if isOk {
		alarmObj.IsAlarm = 1
		alarmObj.AlarmTime = time.Now().Unix()
		alarmObj.Receivers = apm.Receivers.String
		this.monitorPolicyModel.SetLatestAlarmTime(alarmObj.JobID, alarmObj.ServiceName, int(apm.IntervalMinutes*60))
	}

	return nil
}

// 钉钉机器人报警
func (this *Kafka) alarmByDingTalkRobot(
	content string, msg *sarama.ConsumerMessage, apm *model.AlarmPolicy) (bool, error) {
	if msg == nil || apm == nil || apm.Receivers.String == "" {
		return false, errors.New("params error, apm is nil or receivers is empty")
	}

	var respBody struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	var reqBody struct {
		MsgType string `json:"msgtype"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
	}

	reqBody.MsgType = "text"
	reqBody.Text.Content = content
	b, err := json.Marshal(reqBody)
	if err != nil {
		return false, err
	}

	var isOk bool
	var errAlarm error
	receivers := strings.Split(apm.Receivers.String, ",")
	for _, receiver := range receivers {
		url := strings.TrimSpace(receiver)
		resp, err := http.Post(url, "application/json;charset=UTF-8", bytes.NewReader(b))
		if err != nil {
			errAlarm = err
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			errAlarm = err
			continue
		}
		if err = json.Unmarshal(body, &respBody); err != nil {
			errAlarm = err
			continue
		}
		if resp.StatusCode == http.StatusOK && respBody.ErrCode == 0 {
			isOk = true
			errAlarm = nil
			seelog.Infof("alarm robot: %s success. [T:%s P:%d O:%d M:%s]",
				url, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		} else {
			errAlarm = errors.New("ding talk alarm error, err_msg: " + respBody.ErrMsg)
			seelog.Errorf("alarm robot: %s error, resp err_code: %d, err_msg: %s, [T:%s P:%d O:%d M:%s]",
				url, respBody.ErrCode, respBody.ErrMsg, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		}
	}

	return isOk, errAlarm
}
