package queue

import (
	"container/list"
	"sync"
)

// Queue 通用队列结构
type Queue[T any] struct {
	items list.List
	mutex sync.RWMutex
}

// NewQueue 创建新队列
func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		items: list.List{},
	}
}

// Enqueue 入队操作
func (q *Queue[T]) Offer(item T) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.items.PushBack(item)
}

// Dequeue 出队操作
func (q *Queue[T]) Poll() T {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.items.Len() == 0 {
		panic("poll from empty queue")
	}

	front := q.items.Front()
	q.items.Remove(front)
	return front.Value.(T)
}

// Peek 查看队列头元素
func (q *Queue[T]) First() T {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	if q.items.Len() == 0 {
		panic("first from empty queue")
	}

	return q.items.Front().Value.(T)
}

// Size 获取队列大小
func (q *Queue[T]) Size() int {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	return q.items.Len()
}

// IsEmpty 检查队列是否为空
func (q *Queue[T]) IsEmpty() bool {
	return q.Size() == 0
}
