// Package runner abstracts "run a command on the Docker host / inside a container"
// so the fleet logic can be driven by real Docker or by an in-memory mock cluster.
package runner

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Result is the outcome of one command.
type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Duration time.Duration
}

// ExitError is returned when a command exits non-zero.
type ExitError struct {
	Cmd    string
	Result Result
}

func (e *ExitError) Error() string {
	msg := strings.TrimSpace(e.Result.Stderr)
	if msg == "" {
		msg = strings.TrimSpace(e.Result.Stdout)
	}
	return fmt.Sprintf("%s: exit %d: %s", e.Cmd, e.Result.ExitCode, msg)
}

// Runner executes commands. Both methods return a *ExitError for non-zero exits.
type Runner interface {
	// Docker runs `docker <args...>` on the host.
	Docker(ctx context.Context, args ...string) (Result, error)
	// Exec runs argv inside a running container (`docker exec`).
	Exec(ctx context.Context, container string, argv ...string) (Result, error)
}

// Docker is the real driver. It shells out to the docker CLI.
type DockerRunner struct {
	Bin string // defaults to "docker"
}

func (d DockerRunner) bin() string {
	if d.Bin == "" {
		return "docker"
	}
	return d.Bin
}

func (d DockerRunner) Docker(ctx context.Context, args ...string) (Result, error) {
	start := time.Now()
	cmd := exec.CommandContext(ctx, d.bin(), args...)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err := cmd.Run()
	res := Result{Stdout: so.String(), Stderr: se.String(), Duration: time.Since(start)}
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			res.ExitCode = ee.ExitCode()
			return res, &ExitError{Cmd: "docker " + firstArgs(args), Result: res}
		}
		return res, err
	}
	return res, nil
}

func (d DockerRunner) Exec(ctx context.Context, container string, argv ...string) (Result, error) {
	return d.Docker(ctx, append([]string{"exec", container}, argv...)...)
}

func firstArgs(args []string) string {
	if len(args) > 2 {
		args = args[:2]
	}
	return strings.Join(args, " ")
}
