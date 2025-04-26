package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

/**
用ctx管理worker，超时时，pool抛出异常，所有worker都关闭
*/

func main() {
	pool := NewWorkerPool(5)
	pool.Run()

	const taskNum = 100
	go func() {
		for i := 0; i < taskNum; i++ {
			pool.Submit(i)
		}
		close(pool.taskChan)
	}()

	err := pool.WaitWithTimeout(taskNum, 1*time.Second)
	// pool超时，调用cancel函数，通知所有worker关闭
	if err != nil {
		fmt.Println(err)
		pool.cancel()
	} else {
		fmt.Println("所有任务执行完成")
	}
}

type WorkerPool struct {
	workerCnt int
	taskChan  chan int
	done      chan struct{}
	ctx       context.Context    // 上下文，统一管理所有worker
	cancel    context.CancelFunc // 取消函数，让所有worker退出
}

func NewWorkerPool(workerCnt int) *WorkerPool {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &WorkerPool{
		workerCnt: workerCnt,
		taskChan:  make(chan int),
		done:      make(chan struct{}),
		ctx:       ctx,
		cancel:    cancelFunc,
	}
}

func (p *WorkerPool) Run() {
	for i := 0; i < p.workerCnt; i++ {
		// 开启worker
		go func(i int) {
			for {
				select {
				// worker监听ctx中的Done，若有消息，则表明pool要求其停止
				case <-p.ctx.Done():
					fmt.Println("worker", i, "退出")
					return
				// worker正常工作，取出任务执行
				case task, ok := <-p.taskChan:
					if !ok {
						return
					}
					fmt.Println("worker", i, "开始", task)
					time.Sleep(100 * time.Millisecond)
					fmt.Println("worker", i, "完成", task)
					p.done <- struct{}{}
				}
			}
		}(i)
	}
}

// 提交任务
func (p *WorkerPool) Submit(task int) {
	p.taskChan <- task
}

// 线程池等待所有任务完成
func (p *WorkerPool) Wait(total int) {
	var cnt int
	// 阻塞等待done通道中传来消息：如果没有消息，则阻塞；否则将cnt++
	// -> 逐个确认了完成的任务数量
	for range p.done {
		cnt++
		if cnt == total {
			return
		}
	}
}

// 超时等待
func (p *WorkerPool) WaitWithTimeout(total int, duration time.Duration) error {
	ctx, cancel := context.WithTimeout(p.ctx, duration)
	defer cancel()

	var cnt int
	for {
		select {
		// 超时
		case <-ctx.Done():
			return errors.New("超时")
		// 没超时，收到1个任务结束的通知
		case <-p.done:
			cnt++
			if cnt == total {
				return nil
			}
		}
	}
}
