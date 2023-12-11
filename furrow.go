//go:build linux

package main

import (
	"context"
	"furrow_next_pl/jobs"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"furrow_next_pl/broker"
	"furrow_next_pl/furrow"

	"github.com/namsral/flag"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	log "github.com/sirupsen/logrus"
)

var (
	dockerEndpoint string
	dockerUsername string
	dockerPassword string

	beanstalkHost string
	jobsTube      string

	workers        int
	publishMetrics string
)

// Job is for passing jobs from reader to worker
type Job struct {
	ctx context.Context
	job *furrow.Job
}

func init() {
	flag.StringVar(&dockerEndpoint, "docker-host", "/run/containerd/containerd.sock", "Address to Containerd host")
	flag.StringVar(&beanstalkHost, "beanstalk-host", "beanstalk:11300", "Address and port to beanstalkd")
	flag.StringVar(&jobsTube, "tube", "jobs", "Name of tube to read jobs from")
	flag.IntVar(&workers, "workers", 1, "Number of jobs to process at once")
	flag.StringVar(&dockerUsername, "docker-username", "", "Username for access to Docker hub")
	flag.StringVar(&dockerPassword, "docker-password", "", "Password for username")
	flag.StringVar(&publishMetrics, "publish-metrics", "", "Bind address/port to publish metrics on")
}

func connectToK8s() *kubernetes.Clientset {
	//env := "local"
	env := os.Getenv("IS_LOCAL_ENV")
	log.WithFields(log.Fields{
		"env": env,
	}).Info("Env exec, empty means its running on cluster")
	if len(env) > 0 {
		var kubeconfig *string
		if home := homedir.HomeDir(); home != "" {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else {
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		}
		flag.Parse()

		config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			panic(err.Error())
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			panic(err.Error())
		}
		return clientset
	} else {
		config, err := rest.InClusterConfig()
		if err != nil {
			log.Panicln("Failed to get in-cluster config")
			log.Panicln(err)
		}

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			log.Panicln("Failed to create K8s clientset")
		}

		return clientset
	}
}

func main() {
	flag.Parse()

	furrow.Version()

	if dockerEndpoint == "" || beanstalkHost == "" || jobsTube == "" {
		help()
	}

	client := connectToK8s()

	//log.WithField("DOCKER_HOST", dockerEndpoint).Info("Connecting to Containerd")
	//client, err := containerd.New(dockerEndpoint, containerd.WithDefaultNamespace("furrow_docker"))
	//if err != nil {
	//	log.Error("Cannot connect to containerd socket.", err)
	//	return
	//}
	//
	//state := client.Conn().GetState()
	//log.Info("Connection state to containerd: " + state.String())

	stop := make(chan struct{}, 1)
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		sig := <-signals

		log.Infof("Got signal %v, shutting down workers and cancelling running jobs.\n", sig)

		close(stop)

		<-time.After(time.Second * 5)
		log.Info("Force exiting ...")
		os.Exit(1)
	}()

	runner := jobs.NewRunner(client, dockerUsername, dockerPassword)
	go runner.Start()
	//	defer runner.Stop()

	jobTimer := metrics.GetOrRegisterTimer("job.execute_time", metrics.DefaultRegistry)
	jobCounter := metrics.GetOrRegisterCounter("job.executed", metrics.DefaultRegistry)
	if publishMetrics != "" {
		log.Infof("Publishing metrics to %s\n", publishMetrics)
		go func() {
			exp.Exp(metrics.DefaultRegistry)
			log.Warn(http.ListenAndServe(publishMetrics, http.DefaultServeMux))
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			logFields := log.Fields{
				"WORKER":          worker,
				"BEANSTALKD_HOST": beanstalkHost,
				"TUBE":            jobsTube,
			}
			log.WithFields(logFields).Info("Connecting to beanstalkd")
			b, err := broker.New(beanstalkHost, jobsTube)
			if err != nil {
				log.Fatal(err)
			}

			defer func() {
				log.WithFields(logFields).Info("Worker has stopped")
				b.Close()
				wg.Done()
			}()

			getNextJob := make(chan struct{}, 1)
			jobs := make(chan Job, 1)
			go func() {
				var cancel context.CancelFunc
				cancel = func() {}
				for {
					select {
					case <-stop:
						cancel()
						return
					case <-getNextJob:
						go func() {
							job := Job{}
							ctx := context.Background()
							job.ctx, job.job = b.GetJob(ctx)
							// Should only get nil job because of reserve timeouts.
							// It is therefore safe to just wait again for the next.
							if job.job == nil {
								getNextJob <- struct{}{}
								return
							}
							cancel, _ = broker.CancelFunc(job.ctx)
							jobs <- job

							jobCounter.Inc(1)
						}()
					}
				}
			}()

			for {
				getNextJob <- struct{}{}
				select {
				case <-stop:
					return
				case job := <-jobs:
					jobTimer.Time(func() {
						status := runner.Run(job.ctx, job.job)
						b.Finish(job.ctx, status)
					})
				}
			}

		}(i + 1)
	}
	wg.Wait()
}

func help() {
	flag.Usage()
	os.Exit(1)
}
