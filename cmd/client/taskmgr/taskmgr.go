package taskmgr

import (
	"container/list"
	"errors"
	"sync"
)

type HandlerFunc func(ctx any, taskContext any) error

type task struct {
	running      bool
	taskContexts list.List
}

type TaskManager struct {
	contexts     map[string]any
	mutex        sync.Mutex
	taskContexts map[string]*task
	handler      HandlerFunc
	running      bool
	sendError    func(error)
	wg           sync.WaitGroup
	close        chan struct{}
}

func NewTaskManager(handler HandlerFunc) *TaskManager {
	return &TaskManager{
		contexts:     make(map[string]any),
		taskContexts: make(map[string]*task),
		handler:      handler,
		close:        make(chan struct{}),
	}
}

func (t *TaskManager) AddContext(key string, ctx any) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.contexts[key] = ctx
}

func (t *TaskManager) GetContext(key string) any {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.contexts[key]
}

func (t *TaskManager) GetOrNewContext(key string, newContextFunc func() any) any {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if ctx, ok := t.contexts[key]; ok {
		return ctx
	}
	ctx := newContextFunc()
	t.contexts[key] = ctx
	return ctx
}

func (t *TaskManager) AddTaskContext(key string, taskContext any) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if tsk, ok := t.taskContexts[key]; ok {
		tsk.taskContexts.PushBack(taskContext)
	} else {
		tsk = &task{
			running:      false,
			taskContexts: list.List{},
		}
		tsk.taskContexts.PushBack(taskContext)
		t.taskContexts[key] = tsk
	}
	if t.running && !t.taskContexts[key].running {
		// run the task
		t.wg.Add(1)
		go t.runTask(key)
	}
}

func (t *TaskManager) runTask(key string) {
	defer t.wg.Done()
	t.mutex.Lock()
	tsk := t.taskContexts[key]
	tsk.running = true
	ctx := t.contexts[key]
	for {
		if tsk.taskContexts.Len() == 0 {
			tsk.running = false
			t.mutex.Unlock()
			return
		}
		taskContext := tsk.taskContexts.Remove(tsk.taskContexts.Front())
		t.mutex.Unlock()
		err := t.handler(ctx, taskContext)
		if err != nil {
			t.sendError(err)
		}
		t.mutex.Lock()
	}
}

func (t *TaskManager) Run() error {
	errs := make([]error, 0)
	t.sendError = func(err error) {
		if err == nil {
			return
		}
		t.mutex.Lock()
		errs = append(errs, err)
		t.mutex.Unlock()
	}
	t.mutex.Lock()
	t.running = true
	for key := range t.taskContexts {
		t.wg.Add(1)
		go t.runTask(key)
	}
	t.mutex.Unlock()

	t.wg.Wait()
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.running = false
	// gather errors
	return errors.Join(errs...)
}
