package main

import (
	"errors"
	"fmt"
	"time"
)

func main() {
	pool := NewWorkerPool(5)
	pool.Run()

	const taskNum = 100
	go func() {
		for i := 0; i < taskNum; i++ {
			pool.Submit(i)
		}
	}()
	//pool.Wait(taskNum)
	err := pool.WaitWithTimeout(taskNum, 1*time.Second)
	if err != nil {
		fmt.Println(err)
		return
	}
}

type WorkerPool struct {
	workerCnt int
	taskChan  chan int
	done      chan struct{}
}

func NewWorkerPool(workerCnt int) *WorkerPool {
	return &WorkerPool{
		workerCnt: workerCnt,
		taskChan:  make(chan int),
		done:      make(chan struct{}),
	}
}

func (p *WorkerPool) Run() {
	for i := 0; i < p.workerCnt; i++ {
		// 开启worker
		go func(i int) {
			select {}

			// 从taskChan中取任务
			for task := range p.taskChan {
				time.Sleep(100 * time.Millisecond)
				fmt.Println("worker", i, "finished", task)
				p.done <- struct{}{}
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
	var cnt int
	timeout := time.After(duration)

	for {
		select {
		// 超时
		case <-timeout:
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
