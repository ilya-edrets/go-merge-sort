package task

import "sync"

type Awaitable interface {
	getWaitGroup() *sync.WaitGroup
}

type Task[T any] struct {
	wg     sync.WaitGroup
	result T
	err    error
}

func Run[T any](f func() (T, error)) *Task[T] {
	t := &Task[T]{}
	t.wg.Add(1)

	go func() {
		defer t.wg.Done()
		t.result, t.err = f()
	}()

	return t
}

func WaitAll(tasks ...Awaitable) {
	for _, task := range tasks {
		task.getWaitGroup().Wait()
	}
}

func (t *Task[T]) Await() (T, error) {
	t.wg.Wait()
	return t.result, t.err
}

func (t *Task[T]) getWaitGroup() *sync.WaitGroup {
	return &t.wg
}
