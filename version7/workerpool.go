package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"workerpool/utils"
)

/**
1. doneChan带缓冲
*/

func main() {
	pool := NewWorkerPool(5)
	go pool.Run()

	const taskNum = 1000
	var resultChans []<-chan Result

	go func() {
		for i := 0; i < taskNum; i++ {
			idx := i

			resultChan, err := pool.SubmitWithTimeout(func(ctx context.Context) (interface{}, error) {
				fmt.Println("任务处理中...", idx)
				time.Sleep(100 * time.Millisecond)
				return fmt.Sprintf("任务%d完成", idx), nil
			}, 2*time.Second, 120*time.Millisecond)

			if err != nil {
				fmt.Println("任务提交失败:", i, err)
			} else {
				resultChans = append(resultChans, resultChan)
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
	//pool.Stop()

	// 收集所有结果
	for _, ch := range resultChans {
		res := <-ch
		if res.err != nil {
			fmt.Println("任务出错:", res.err)
		} else {
			fmt.Println("收到任务结果:", res.value)
		}
	}
}

type Task struct {
	fn         func(ctx context.Context) (interface{}, error)
	resultChan chan Result
	timeout    time.Duration
}

type Result struct {
	value interface{}
	err   error
}

type WorkerPool struct {
	maxWorkers int

	taskChan     chan Task
	workerQueue  chan Task
	waitingQueue *queue.Queue[Task]
	doneChan     chan struct{} // 每当一个任务完成，worker就向其中发一个消息

	active int // 活跃worker数量
	mu     sync.Mutex
	wg     sync.WaitGroup

	ctx    context.Context    // 上下文，统一管理所有worker
	cancel context.CancelFunc // 取消函数，通知所有worker退出
}

func NewWorkerPool(maxWorkers int) *WorkerPool {
	ctx, cancelFunc := context.WithCancel(context.Background())
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	return &WorkerPool{
		maxWorkers:   maxWorkers,
		taskChan:     make(chan Task, 1000),
		workerQueue:  make(chan Task, maxWorkers),
		waitingQueue: queue.NewQueue[Task](),
		doneChan:     make(chan struct{}, 1000),
		ctx:          ctx,
		cancel:       cancelFunc,
	}
}

func (p *WorkerPool) Run() {

	for {
		if p.waitingQueue.Size() != 0 {
			if !p.processWaitingQueue() {
				break
			}
			continue
		}

		select {
		// pool关闭，直接退出
		case <-p.ctx.Done():
			return
		// 取出一个任务
		case task, ok := <-p.taskChan:
			// taskChan关闭，退出
			if !ok {
				break
			}
			select {
			// 传给workerQueue
			case p.workerQueue <- task:
			default:
				// worker数量未满，还能继续创建
				if p.active < p.maxWorkers {
					p.startWorker(task)
				} else {
					p.waitingQueue.Offer(task)
				}
			}
		}
	}
}

// 处理等待队列中的Task
func (p *WorkerPool) processWaitingQueue() bool {
	select {
	case <-p.ctx.Done():
		return false
	case task, ok := <-p.taskChan:
		// taskChan已关闭
		if !ok {
			return false
		}
		p.waitingQueue.Offer(task)
	case p.workerQueue <- p.waitingQueue.First():
		p.waitingQueue.Poll()
	}
	return true
}

// 启动一个worker
func (p *WorkerPool) startWorker(task Task) {
	fmt.Println("worker启动")
	p.mu.Lock()
	p.active++
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
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()

		// 处理任务
		for {
			if curTask.fn != nil {
				p.handleTask(curTask)
			}

			// 处理完成
			select {
			// 1:获取下一个任务
			case nextTask := <-p.workerQueue:
				curTask = nextTask
				// 重置计时器
				timer.Reset(5 * time.Second)
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

// 处理task
func (p *WorkerPool) handleTask(task Task) {
	// 执行任务并传递结果
	fmt.Println("worker开始处理任务")
	ctx := p.ctx
	if task.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(p.ctx, task.timeout)
		defer cancel()
	}

	result, err := task.fn(ctx)

	// 执行结果
	select {
	// 超时或取消
	case <-ctx.Done():
		task.resultChan <- Result{nil, errors.New("任务超时或取消")}
	// 正常执行完成
	default:
		task.resultChan <- Result{result, err}
	}

	p.doneChan <- struct{}{}
	fmt.Println("worker处理任务结束")
}

// 提交任务
func (p *WorkerPool) Submit(fn func(ctx context.Context) (interface{}, error), executeTimeout time.Duration) (<-chan Result, error) {
	resultChan := make(chan Result, 1)

	select {
	// 成功提交任务
	case p.taskChan <- Task{
		fn:         fn,
		resultChan: resultChan,
		timeout:    executeTimeout,
	}:
		return resultChan, nil
	default:
		return nil, errors.New("通道关闭或阻塞")
	}
}

// 提交任务，带超时（如果一定时间内没有worker能处理，则抛出异常）
func (p *WorkerPool) SubmitWithTimeout(fn func(ctx context.Context) (interface{}, error), submitTimeout, executeTimeout time.Duration) (<-chan Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), submitTimeout)
	defer cancel()

	resultChan := make(chan Result, 1)
	select {
	// 成功提交任务
	case p.taskChan <- Task{
		fn:         fn,
		resultChan: resultChan,
		timeout:    executeTimeout,
	}:
		return resultChan, nil
	// 任务提交超时
	case <-ctx.Done():
		return nil, errors.New("提交任务超时")
	default:
		return nil, errors.New("通道关闭或阻塞")
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
