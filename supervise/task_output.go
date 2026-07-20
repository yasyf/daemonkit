package supervise

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
)

type taskOutputPipe struct {
	name   string
	reader *os.File
	writer *os.File
	target io.Writer
	done   chan error
}

type taskOutputs struct {
	mu      sync.Mutex
	pipes   []*taskOutputPipe
	started bool
}

func newTaskOutputs(cmd *exec.Cmd, stdout, stderr io.Writer) (*taskOutputs, error) {
	outputs := &taskOutputs{}
	stdoutPipe, err := outputs.add("stdout", stdout)
	if err != nil {
		return nil, err
	}
	stderrPipe, err := outputs.add("stderr", stderr)
	if err != nil {
		outputs.closeUnstarted()
		return nil, err
	}
	if stdoutPipe != nil {
		cmd.Stdout = stdoutPipe.writer
	}
	if stderrPipe != nil {
		cmd.Stderr = stderrPipe.writer
	}
	return outputs, nil
}

func (o *taskOutputs) add(name string, target io.Writer) (*taskOutputPipe, error) {
	if target == nil {
		return nil, nil
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("supervise: worker %s pipe: %w", name, err)
	}
	pipe := &taskOutputPipe{name: name, reader: reader, writer: writer, target: target, done: make(chan error, 1)}
	o.pipes = append(o.pipes, pipe)
	return pipe, nil
}

func (o *taskOutputs) start() {
	o.started = true
	for _, pipe := range o.pipes {
		_ = pipe.writer.Close()
		go func(pipe *taskOutputPipe) {
			defer pipe.reader.Close()
			_, err := io.Copy(lockedTaskOutput{o: o, target: pipe.target}, pipe.reader)
			if err != nil {
				err = fmt.Errorf("supervise: copy worker %s: %w", pipe.name, err)
			}
			pipe.done <- err
		}(pipe)
	}
}

func (o *taskOutputs) closeUnstarted() {
	for _, pipe := range o.pipes {
		_ = pipe.reader.Close()
		_ = pipe.writer.Close()
	}
}

func (o *taskOutputs) wait() error {
	if !o.started {
		o.closeUnstarted()
		return nil
	}
	var err error
	for _, pipe := range o.pipes {
		err = errors.Join(err, <-pipe.done)
	}
	return err
}

type lockedTaskOutput struct {
	o      *taskOutputs
	target io.Writer
}

func (w lockedTaskOutput) Write(payload []byte) (int, error) {
	w.o.mu.Lock()
	defer w.o.mu.Unlock()
	return w.target.Write(payload)
}
