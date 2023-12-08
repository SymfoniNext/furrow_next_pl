package jobs

import (
	"errors"
	"github.com/SymfoniNext/furrow_next_pl/broker"
	"github.com/SymfoniNext/furrow_next_pl/furrow"
	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/oci"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/opencontainers/runtime-spec/specs-go"
	"net/http"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"

	"context"
)

var (
	// Directory names to use for the in and out mounts in the container
	volumeInMount  = "/in"
	volumeOutMount = "/out"

	ErrEmptyJob = errors.New("Empty job")
)

// Runner for executing jobs via Docker & containers
type jobRunner struct {
	// containerd client
	client *containerd.Client

	// Container removal is queued for all clients via this channel
	containerRemoval chan string

	// Docker hub username
	username string
	// Docker hub password
	password string
}

func NewRunner(client *containerd.Client, username string, password string) furrow.Runner {
	job := &jobRunner{
		client:           client,
		containerRemoval: make(chan string),
		username:         username,
		password:         password,
	}

	return job
}

func (j jobRunner) Run(ctx context.Context, job *furrow.Job) furrow.JobStatus {
	if job.GetImage() == "" {
		log.Warnf("Received empty job: (%#v)", job)
		return furrow.JobStatus{
			Err: ErrEmptyJob,
			// No point burying an empty job.
			// But perhaps we need more verbose logging?
			Bury: false,
		}
	}

	jobID, _ := broker.JobID(ctx)
	logFields := log.Fields{
		"requestID": job.GetRequestID(),
		"jobID":     jobID,
		"image":     job.GetImage(),
		"cmd":       job.GetCmd(),
	}

	jobStatus := furrow.JobStatus{
		ID:     jobID,
		Notify: job.GetNotify(),
	}

	// Image doesn't exist, so we need to get it
	// how are we pulling private repos?
	//resolver := docker.NewResolver(docker.ResolverOptions{
	//	Hosts: docker.ConfigureDefaultRegistries(
	//		docker.WithPlainHTTP(func(string) (bool, error) {
	//			return true, nil
	//		}),
	//		docker.WithAuthorizer(docker.NewDockerAuthorizer(
	//			docker.WithAuthCreds(func(host string) (string, string, error) {
	//				log.WithFields(log.Fields{
	//					"username": j.username,
	//					"password": j.password,
	//					"host":     host,
	//				}).Info("Trying to access dockerhub")
	//				return j.username, j.password, nil
	//			}),
	//		)),
	//	),
	//})

	//resolver := docker.NewResolver(docker.ResolverOptions{
	//	Hosts: func(host string) ([]docker.RegistryHost, error) {
	//		return []docker.RegistryHost{
	//			{
	//				Client: http.DefaultClient,
	//				Host:   "index.docker.io",
	//				Scheme: "https",
	//				Path:   "v2",
	//				Authorizer: docker.NewDockerAuthorizer(docker.WithAuthCreds(func(host string) (string, string, error) {
	//					log.WithFields(log.Fields{
	//						"username": j.username,
	//						"password": j.password,
	//						"host":     host,
	//					}).Info("Trying to access dockerhub")
	//					return j.username, j.password, nil
	//				})),
	//				Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
	//			},
	//		}, nil
	//	},
	//})
	resolver := docker.NewResolver(docker.ResolverOptions{
		Hosts: func(host string) ([]docker.RegistryHost, error) {
			return []docker.RegistryHost{
				{
					Client: http.DefaultClient,
					Host:   "registry-1.docker.io",
					Scheme: "http",
					Path:   "/v2/" + host,
					Authorizer: docker.NewDockerAuthorizer(docker.WithAuthCreds(func(host string) (string, string, error) {
						return j.username, j.password, nil
						//return "incorrect", "user", nil
					})),
					Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
				},
			}, nil
			//
			//return docker.ConfigureDefaultRegistries()(host)
		},
	})

	log.WithFields(log.Fields{
		"state":  j.client.Conn().GetState().String(),
		"target": j.client.Conn().Target(),
	}).Info("Connection status to DockerHub")

	image, err := j.client.GetImage(ctx, job.GetImage())
	if err != nil {
		image, err = j.client.Pull(ctx, job.GetImage(), containerd.WithResolver(resolver), containerd.WithPullUnpack)
		if err != nil {
			log.WithFields(log.Fields{
				"job":        job,
				"error":      err,
				"image_name": job.GetImage(),
			}).Warn("Error pulling image")

			jobStatus.Err = err
			jobStatus.Bury = true
			return jobStatus
		} else {
			log.WithFields(log.Fields{
				"name":      image.Metadata().Name,
				"createdAt": image.Metadata().CreatedAt,
				"updatedAt": image.Metadata().UpdatedAt,
				"image":     image,
			}).Info("Pulled image meta")
		}
	}
	//
	//images, err := j.client.ListImages(ctx)
	//if err != nil {
	//	return furrow.JobStatus{}
	//} else {
	//	for i := range images {
	//		log.WithFields(log.Fields{
	//			"storedImage":   images[i],
	//			"storedImgMeta": images[i].Metadata(),
	//		}).Info("List of stored images")
	//	}
	//}

	//binds := make([]string, 0)
	//if job.GetVolumes() != nil {
	//	logFields["volumes"] = job.GetVolumes()
	//
	//	log.WithFields(log.Fields{
	//		"IN":  job.Volumes.GetIn() + ":" + volumeInMount,
	//		"OUT": job.Volumes.GetOut() + ":" + volumeOutMount,
	//	}).Info("Janocha debug volumes")
	//	if job.Volumes.GetIn() != "" {
	//		binds = append(binds, job.Volumes.GetIn()+":"+volumeInMount)
	//	}
	//	if job.Volumes.GetOut() != "" {
	//		binds = append(binds, job.Volumes.GetOut()+":"+volumeOutMount)
	//	}
	//
	//}
	//containerd.WithNewSnapshotView("janocha-test", image),
	// option to schedule a service instead?
	log.WithFields(logFields).Info("Creating a container...")

	//ociSpec := oci.WithImageConfigArgs(image, []string{"-in", "/in/msg.eml", "-out", "/out/parsed.pb"})
	ociSpec := oci.WithImageConfigArgs(image, job.GetCmd())

	mounts := make([]specs.Mount, 0)
	if job.GetVolumes() != nil {
		if job.Volumes.GetIn() != "" {
			mounts = append(mounts, specs.Mount{
				Destination: volumeInMount,
				Type:        "rbind",
				Source:      job.Volumes.GetIn(),
				Options:     []string{"rbind", "rw"},
			})
		}
		if job.Volumes.GetOut() != "" {
			mounts = append(mounts, specs.Mount{
				Destination: volumeOutMount,
				Type:        "rbind",
				Source:      job.Volumes.GetOut(),
				Options:     []string{"rbind", "rw"},
			})
		}
	}

	log.WithFields(log.Fields{
		"mounts": mounts,
		"vars":   job.GetEnv(),
	}).Info("Mounting following binds")
	container, err := j.client.NewContainer(
		ctx,
		strconv.FormatUint(jobID, 10),
		containerd.WithImage(image),
		containerd.WithNewSnapshot(strconv.FormatUint(jobID, 10), image),
		containerd.WithNewSpec(
			ociSpec,
			//oci.WithProcessArgs(job.GetCmd()...),
			// -in /in/msg.eml", "-out /out/test.pb
			//oci.WithProcessArgs("java", "-jar", "/email-parser/email-parser.jar"),
			//oci.WithProcessCommandLine("-in /in/msg.eml"),
			oci.WithEnv(job.GetEnv()),
			oci.WithMounts(
				mounts,
				//[]specs.Mount{
				//	{
				//		Destination: "/in",
				//		Type:        "rbind",
				//		Source:      "/furrow/in",
				//		Options:     []string{"rbind", "rw"},
				//	},
				//	{
				//		Destination: "/out",
				//		Type:        "rbind",
				//		Source:      "/furrow/out",
				//		Options:     []string{"rbind", "rw"},
				//	},
				//},
			),
		),
	)
	if err != nil {
		log.WithFields(logFields).WithError(err).Error("Failed to create container")
	}

	//defer container.Delete(ctx, containerd.WithSnapshotCleanup)

	if err != nil {
		log.WithFields(logFields).Warn(err)
		jobStatus.Err = err
		jobStatus.Bury = true
		return jobStatus
	}

	logFields["container_id"] = container.ID()

	log.WithFields(logFields).Info("Starting container")

	stream := cio.NewCreator(cio.WithStdio)
	task, taskErr := container.NewTask(ctx, stream) //containerd.WithRootFS(),

	if taskErr != nil {
		log.WithFields(log.Fields{
			"error":        taskErr,
			"container_id": container.ID(),
		}).Error("Failed to create new task for container...")

		jobStatus.Err = taskErr
		jobStatus.Bury = true
		return jobStatus
	}
	//defer task.Delete(ctx)

	workDone := make(chan struct{}, 1)
	// Want to pick up context cancellations
	errorOccurred := make(chan error, 1)

	if err := task.Start(ctx); err != nil {
		log.WithFields(logFields).Warn(err)
		jobStatus.Err = err
		jobStatus.Bury = true
	}
	// But still extract any possibly relevant info for the caller
	go func() {
		log.WithFields(logFields).Info("Waiting for container to run")

		// exit code handler - optional code to clean up if failing
		exitCode, taskErr := task.Wait(ctx)
		if taskErr != nil {
			errorOccurred <- taskErr
			return
		}
		exitCodeStatus := <-exitCode
		if exitCodeStatus.ExitCode() != 0 {
			jobStatus.ExitCode = int(exitCodeStatus.ExitCode())
			jobStatus.Bury = true
			errorOccurred <- errors.New("None zero exit")
			return
		}

		// if notify (handled by broker)

		log.WithFields(logFields).Info("Attaching to container")

		log.WithFields(logFields).Info("Removing container")
		j.containerRemoval <- container.ID()

		workDone <- struct{}{}
	}()

	select {
	case <-workDone:
		// Wait until container is done
	case err := <-errorOccurred:
		log.WithFields(logFields).Warn(err)
		jobStatus.Bury = true
		jobStatus.Err = err
	case <-ctx.Done():
		// Remove container?
		log.WithFields(logFields).Warn("Job cancelled after request.  Stopping container.")
		if err := container.Delete(ctx); err != nil {
			log.WithFields(logFields).Warn(err)
		}
		jobStatus.Bury = true
		jobStatus.Err = ctx.Err()
	}

	return jobStatus
}

func (j jobRunner) Start() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for id := range j.containerRemoval {
			err := j.client.ContainerService().Delete(context.Background(), id)
			if err != nil {
				log.WithField("id", id).WithField("error", err).Warn("Unable to delete container")
			}
		}
	}()

	wg.Wait()
}
