package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/jmyounker/mustache"
	"os/exec"
	"io/ioutil"
	"syscall"
	"errors"
)

var version string
var Debug bool = false

const OUTCOME_SUCCESS string = "SUCCESS"
const OUTCOME_FAILURE string = "FAILURE"
const OUTCOME_TIMEOUT string = "TIMEOUT"

func main() {
	err := NewApp().Run(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type App struct {
	Prog string
	Parallelism int
	Args []string
}

const DEFAULT_PARALLELISM = 8

func NewApp() *App{
	return &App{
		Parallelism: DEFAULT_PARALLELISM,
	}
}

func (a *App)Run(argv []string) error {
	args := []string{}
	a.Prog = argv[0]
	i := 1
	for i < len(argv) {
		x := argv[i]
		switch x {
		case "-p", "--parallelism":
			i = i + 1
			p, err := strconv.Atoi(argv[i])
			if err != nil {
				return err
			}
			a.Parallelism = p
			i = i + 1
		case "-d", "--debug":
			i = i + 1
			Debug = true
		case "-v", "--version":
			i = i + 1
			fmt.Println(version)
			return nil
		case "-h", "--help":
			i = i + 1
			fmt.Printf("usage: %s [--parallelism N] [--debug] CMD\n", a.Prog)
			return nil
		default:
			args = append(args, argv[i])
			i = i + 1
		}
	}
	a.Args = args
	return ActionCmd(a)
}

const RETURNCODE_FAILURE = -4242

func ActionCmd(a *App) error {
	if a.Parallelism < 1 {
		return errors.New("at least one worker required")
	}
	cmd := []*mustache.Template{}
	for _, arg := range(a.Args) {
		t, err := mustache.ParseString(arg)
		if err != nil {
			return nil
		}
		cmd = append(cmd, t)
	}
	jobs := make(chan Job)
	results := make(chan Output)
	inputDone := make(chan struct{})
	workerDone := make(chan struct{})
	outputDone := make(chan struct{})
	// Launch workers
	for i := 0; i < a.Parallelism; i++ {
		go worker(i, cmd, jobs, results, workerDone)
	}
	// Display results from workers
	go func() {
		// Feed input to workers
		j := ReadJsonStream(os.Stdin)
		for x := range j {
			if x.Err == nil {
				jobs <- Job{Value: x.Value}
			} else {
				r := map[string]interface{}{}
				r["cmd"] = []string{}
				r["error"] = fmt.Sprintf("parse error: string(x.Err)")
				r["returncode"] = RETURNCODE_FAILURE
				r["stdout"] = ""
				r["stderr"] = ""
				r["outcome"] = OUTCOME_FAILURE
				results <- Output{Value: r}
			}
		}
		inputDone <- struct{}{}
	}()
	// Wait for input to complete.
	go func() {
		for x := range results {
			if x.Done {
				break
			} else {
				out, err := json.Marshal(x.Value)
				if err != nil {
					log.Panicf("Cannot marshal: %s", x)
				}
				os.Stdout.Write(out)
			}
		}
		outputDone <- struct{}{}
	}()
	waitForTermination(inputDone, 1)
	// Tell workers that there is no more work.  Workers will
	// now quit.
	for i := 0; i < a.Parallelism; i++ {
		jobs <- Job{Done: true}
	}
	// Wait for workers to complete their current tasks.
	waitForTermination(workerDone, a.Parallelism)
	// Tell output routine that there is nothing left. Output
	// routine will now quit.
	results <- Output{Done: true}
	return nil
}


func logf(format string, a ...interface{}) {
	msg, _ := json.Marshal(map[string]string{"message": fmt.Sprintf(format, a)})
	fmt.Print(string(msg))
}

func waitForTermination(done chan struct{}, count int) {
	completed := 0
	for _ = range done {
		completed = completed + 1
		if completed == count {
			return
		}
	}
}

func worker(
	id int,
	cmd []*mustache.Template,
	jobs chan Job,
	completed chan Output,
	done chan struct{}) {
	for job := range(jobs) {
		if job.Done {
			done <- struct{}{}
			return
		}
		r := runJob(cmd, job.Value)
		if Debug {
			r["worker-id"] = id
		}
		completed <- Output{Value: r}
	}
}

func runJob(cmd []*mustache.Template, job interface{}) map[string]interface{} {
	r := map[string]interface{}{}
	r["e"] = job
	args := instantiateArgs(cmd, job)
	r["command"] = args
	r["returncode"] = RETURNCODE_FAILURE
	r["stdout"] = ""
	r["stderr"] = ""
	r["outcome"] = OUTCOME_FAILURE
	prog, err := exec.LookPath(args[0])
	if err != nil {
		r["error"] = fmt.Sprintf("cannot locate command %s: %s", args[0], err)
		return r
	}
	if Debug {
		r["prog"] = prog
	}
	c := exec.Cmd{
		Path: prog,
		Args: args,
	}
	outRdr, err := c.StdoutPipe()
	if err != nil {
		r["error"] = fmt.Sprintf("cannot construct stdout: %s", err)
		return r
	}
	errRdr, err := c.StderrPipe()
	if err != nil {
		r["error"] = fmt.Sprintf("cannot construct stderr: %s", err)
		return r
	}
	err = c.Start()
	if err != nil {
		r["error"] = fmt.Sprintf("failed to launch cmd: %s", err)
		return r
	}
	stdout := make(chan StringWithError)
	stderr := make(chan StringWithError)
	go func() {
		out, err := ioutil.ReadAll(outRdr)
		stdout <- StringWithError{string(out), err}
		close(stdout)
	}()
	go func() {
		out, err := ioutil.ReadAll(errRdr)
		stderr <- StringWithError{string(out), err}
		close(stderr)
	}()
	sout := <- stdout
	serr := <- stderr
	r["stdout"] = sout.Value
	r["stderr"] = serr.Value
	if sout.Err != nil {
		r["error"] = fmt.Sprintf("stdout: %s", sout.Err.Error())
	}
	if serr.Err != nil {
		msg := fmt.Sprintf("stderr: %s", serr.Err.Error())
		err, ok := r["error"]
		if ok {
			r["error"] = fmt.Sprintf("%s; %s", err, msg)
		} else {
			r["error"] = msg
		}
	}
	c.Wait()
	stat := c.ProcessState.Sys().(syscall.WaitStatus)
	r["returncode"] = uint32(stat)
	r["outcome"] = OUTCOME_SUCCESS
	return r
}


func instantiateArgs(cmd []*mustache.Template, params interface{}) []string {
	r := []string{}
	for _, t := range(cmd) {
		r = append(r, t.Render(false, params))
	}
	return r
}

func ReadJsonStream(stream *os.File) chan JsonRead {
	dec := json.NewDecoder(stream)
	out := make(chan JsonRead)
	var j interface{}
	go func() {
		for {
			if err := dec.Decode(&j); err != nil {
				if err == io.EOF {
					close(out)
					return
				} else {
					out <- JsonRead{nil, err}
					close(out)
					return
				}
			}
			out <- JsonRead{j, nil}
		}
	}()
	return out
}

type JsonRead struct {
	Value interface{}
	Err   error
}

type StringWithError struct {
	Value string
	Err error
}

type Job struct {
	Value interface{}
	Done bool
}

type Output struct {
	Value interface{}
	Done bool
}