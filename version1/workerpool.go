package main

import (
	"fmt"
	"time"
)

func main() {
	const jobNum = 5
	taskChan := make(chan int, jobNum)
	results := make(chan int, jobNum)
	// 开启3个worker
	for w := 1; w <= 3; w++ {
		go worker(w, taskChan, results)
	}

	// 5个job
	for j := 1; j <= jobNum; j++ {
		taskChan <- j
	}
	close(taskChan)

	// 获取5个results
	for r := 1; r <= jobNum; r++ {
		fmt.Println(<-results)
	}
}

// 执行任务，并返回结果
func worker(id int, taskChan <-chan int, results chan<- int) {
	for j := range taskChan {
		//fmt.Println("worker", id, "started job", j)
		time.Sleep(time.Second)
		fmt.Println("worker", id, "finished job", j)
		results <- j * 2
	}
}
