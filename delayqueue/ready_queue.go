package delayqueue

import (
	"fmt"

	"github.com/ouqiang/delay-queue/config"
)

type ReadyQueue struct{}

// 将消息放到 redis
// http://www.redis.cn/commands/rpush.html
// queueName:Topic
func pushToReadyQueue(queueName string, jobId string) error {
	queueName = fmt.Sprintf(config.Setting.QueueName, queueName)
	_, err := execRedisCommand("RPUSH", queueName, jobId)

	return err
}

func blockPopFromReadyQueue(queues []string, timeout int) (string, error) {
	var args []interface{}
	for _, queue := range queues {
		queue = fmt.Sprintf(config.Setting.QueueName, queue)
		args = append(args, queue)
	}


	args = append(args, timeout)

	// http://www.redis.cn/commands/blpop.html
	value, err := execRedisCommand("BLPOP", args...)
	if err != nil {
		return "", err
	}
	if value == nil {
		return "", nil
	}
	var valueBytes []interface{}
	valueBytes = value.([]interface{})
	if len(valueBytes) == 0 {
		return "", nil
	}
	element := string(valueBytes[1].([]byte))

	return element, nil
}
