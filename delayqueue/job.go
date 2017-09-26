package delayqueue

import (
	"github.com/vmihailenco/msgpack"
)

type Job struct {
	Topic string `json:"topic"` // topic ，唯一
	Id    string `json:"id"`    // job唯一标识ID 。客户端需要保证唯一性。有关联关系的
	Delay int64  `json:"delay"` // 延迟时间, unix时间戳
	TTR   int64  `json:"ttr"`   // 超时时间,TTR的设计目的是为了保证消息传输的可靠性。
	Body  string `json:"body"`  // body
}

// 获取Job 。看看 putJob
func getJob(key string) (*Job, error) {
	value, err := execRedisCommand("GET", key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	byteValue := value.([]byte)
	job := &Job{}
	err = msgpack.Unmarshal(byteValue, job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// 添加Job ，key:jobId
func putJob(key string, job Job) error {
	value, err := msgpack.Marshal(job)
	if err != nil {
		return err
	}
	_, err = execRedisCommand("SET", key, value)

	return err
}

// 删除Job
func removeJob(key string) error {
	_, err := execRedisCommand("DEL", key)

	return err
}
