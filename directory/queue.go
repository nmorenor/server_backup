package directory

import (
	"fmt"
	"sync"
)

type Queue struct {
	queue []string
	lock  sync.RWMutex
}

func NewQueue() *Queue {
	return &Queue{
		queue: make([]string, 0),
	}
}

func (c *Queue) Enqueue(name string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.queue = append(c.queue, name)
}

func (c *Queue) Dequeue() (string, error) {
	if len(c.queue) > 0 {
		c.lock.Lock()
		defer c.lock.Unlock()
		val := c.queue[0]
		c.queue = c.queue[1:]
		return val, nil
	}
	return "", fmt.Errorf("pop Error: Queue is empty")
}

func (c *Queue) Front() (string, error) {
	if len(c.queue) > 0 {
		c.lock.Lock()
		defer c.lock.Unlock()
		return c.queue[0], nil
	}
	return "", fmt.Errorf("peep Error: Queue is empty")
}

func (c *Queue) Size() int {
	return len(c.queue)
}

func (c *Queue) Empty() bool {
	return len(c.queue) == 0
}
