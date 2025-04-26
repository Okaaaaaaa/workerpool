package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

/**
1. doneChan带缓冲
*/

func main() {
	pool := NewWorkerPool(5)
	pool.Run()

	const taskNum = 100
	go func() {
		for i := 0; i < taskNum; i++ {
			idx := i
			err := pool.SubmitWithTimeout(func() {
				fmt.Println("任务处理中...", idx)
				time.Sleep(100 * time.Millisecond)
			}, 2*time.Second)
			if err != nil {
				fmt.Println("任务提交失败:", i, err)
			}
		}
	}()

	// pool 阻塞，等待所有任务完成
	err := pool.WaitWithTimeout(taskNum, 100*time.Second)
	// pool超时，调用cancel函数，通知所有worker关闭
	if err != nil {
		fmt.Println(err)
		pool.cancel()
	} else {
		fmt.Println("所有任务执行完成")
	}

	// 主动关闭pool
	pool.Stop()
}

type Task func()

type WorkerPool struct {
	maxWorkers int

	taskChan chan Task
	doneChan chan struct{} // 每当一个任务完成，worker就向其中发一个消息

	active int // 活跃worker数量
	mu     sync.Mutex
	wg     sync.WaitGroup

	ctx    context.Context    // 上下文，统一管理所有worker
	cancel context.CancelFunc // 取消函数，通知所有worker退出
}

func NewWorkerPool(maxWorkers int) *WorkerPool {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &WorkerPool{
		maxWorkers: maxWorkers,
		taskChan:   make(chan Task, 1000),
		doneChan:   make(chan struct{}, 1000),
		ctx:        ctx,
		cancel:     cancelFunc,
	}
}

func (p *WorkerPool) Run() {
	go func() {
		for {
			select {
			// pool关闭，直接退出
			case <-p.ctx.Done():
				return
			// 有新任务
			case task, ok := <-p.taskChan:
				// taskChan关闭，退出
				if !ok {
					return
				}
				// 执行task
				// case 1: 开启新worker
				if p.active < p.maxWorkers {
					p.startWorker(task)
				} else {
					// case 2: 将task再放回原来队列中，表示此时没有空闲的worker执行
					go func(task Task) {
						select {
						case p.taskChan <- task:
						case <-p.ctx.Done():
						}
					}(task)
				}
			}
		}
	}()
}

// 启动一个worker
func (p *WorkerPool) startWorker(task Task) {
	p.mu.Lock()
	p.active++
	fmt.Println("worker启动")
	p.mu.Unlock()

	p.wg.Add(1)

	go func(curTask Task) {
		defer func() {
			p.mu.Lock()
			p.active--
			p.mu.Unlock()

			p.wg.Done()
		}()

		// 空闲一定时间后，worker自动退出
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()

		// 处理任务
		for {
			fmt.Println("worker开始处理任务")
			curTask()
			fmt.Println("worker处理任务完成")
			p.doneChan <- struct{}{}

			// 处理完成
			select {
			// 1:获取下一个任务
			case nextTask := <-p.taskChan:
				curTask = nextTask
				// 重置计时器
				timer.Reset(10 * time.Second)
			// 2:空闲超时
			case <-timer.C:
				fmt.Println("worker空闲超时，退出")
				return
			// 3:pool通知退出
			case <-p.ctx.Done():
				fmt.Println("worker收到pool通知，退出")
				return
			}
		}
	}(task)
}

// 提交任务
func (p *WorkerPool) Submit(task Task) {
	p.taskChan <- task
}

// 提交任务，带超时（如果一定时间内没有worker能处理，则抛出异常）
func (p *WorkerPool) SubmitWithTimeout(task Task, duration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	select {
	case p.taskChan <- task:
		return nil
	case <-ctx.Done():
		return errors.New("提交任务超时")
	}
}

// 等待（pool等待所有任务执行完毕才退出）
func (p *WorkerPool) Wait(total int) {
	var cnt int
	// 阻塞等待doneChan通道中传来消息：如果没有消息，则阻塞；否则将cnt++
	// -> 逐个确认了完成的任务数量
	for range p.doneChan {
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
		case <-p.doneChan:
			cnt++
			if cnt == total {
				return nil
			}
		}
	}
}

// 主动控制pool关闭
func (p *WorkerPool) Stop() {
	p.cancel()
	close(p.taskChan)
	// 等待所有worker退出
	p.wg.Wait()
	fmt.Println("worker pool优雅退出")
}
