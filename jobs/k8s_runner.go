package jobs

import (
	"context"
	"errors"
	"furrow_next_pl/broker"
	"furrow_next_pl/furrow"
	log "github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes"
	"sync"
)

var (
	// Directory names to use for the in and out mounts in the container
	volumeInMount  = "/in"
	volumeOutMount = "/out"

	ErrEmptyJob = errors.New("Empty job")

	propagation metav1.DeletionPropagation = "Background"
)

// Runner for executing jobs via Docker & containers
type jobRunner struct {
	// containerd client
	client *kubernetes.Clientset

	// Container removal is queued for all clients via this channel
	containerRemoval chan string

	// Docker hub username
	username string
	// Docker hub password
	password string
}

func NewRunner(client *kubernetes.Clientset, username string, password string) furrow.Runner {
	job := &jobRunner{
		client:           client,
		containerRemoval: make(chan string),
		username:         username,
		password:         password,
	}

	return job
}

func (j jobRunner) Run(ctx context.Context, job *furrow.Job) furrow.JobStatus {
	jobID, _ := broker.JobID(ctx)
	jobStatus := furrow.JobStatus{
		ID:     jobID,
		Notify: job.GetNotify(),
	}

	jobs := j.client.BatchV1().Jobs("jobs-ns")
	var backOffLimit int32 = 1

	//hsType := v1.HostPathDirectory

	jobSpec := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-name",
			Namespace: "jobs-ns",
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "gce-mount",
							VolumeSource: v1.VolumeSource{
								//HostPath: &v1.HostPathVolumeSource{
								//	Path: "/furrow/in",
								//	Type: &hsType,
								//},
								//EmptyDir:              nil,
								GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
									PDName:    "furrow-gcepersistentdisk",
									FSType:    "ext4",
									Partition: 0,
									ReadOnly:  false,
								},
							},
						},
						//{
						//	Name: "out-mount",
						//	VolumeSource: v1.VolumeSource{
						//		HostPath: &v1.HostPathVolumeSource{
						//			Path: "/furrow/out",
						//			Type: &hsType,
						//		},
						//	},
						//},
					},
					Containers: []v1.Container{
						{
							Name:            "test-job-name",
							Image:           "symfoni/email-parser:9e7ce4e",
							ImagePullPolicy: v1.PullIfNotPresent,
							Args:            []string{"-in", "/furrow/in/msg.eml", "-out", "/furrow/out/EMAIL-PARSING-1241492.pb"},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "gce-mount",
									ReadOnly:  false,
									MountPath: "/furrow",
								},
								//{
								//	Name:      "out-mount",
								//	ReadOnly:  false,
								//	MountPath: "/out",
								//},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyOnFailure,
					ImagePullSecrets: []v1.LocalObjectReference{
						{
							Name: "regcred",
						},
					},
				},
			},
			BackoffLimit: &backOffLimit,
		},
	}

	_, err := jobs.Create(ctx, jobSpec, metav1.CreateOptions{})

	if err != nil {
		log.Error("Failed to create K8s job.")
		//err := jobs.Delete(ctx, jobSpec.Name, metav1.DeleteOptions{
		//	PropagationPolicy: &propagation,
		//})
		if err != nil {
			log.Error(err)
		}
		log.Error(err)
		jobStatus.Err = err
		jobStatus.Bury = true
		return jobStatus
	}

	log.Println("Created K8s job successfully")
	return jobStatus
}

func (j jobRunner) Start() {
	var wg sync.WaitGroup

	wg.Add(1)
	//go func() {
	//	defer wg.Done()
	//	for id := range j.containerRemoval {
	//		err := j.client.ContainerService().Delete(context.Background(), id)
	//		if err != nil {
	//			log.WithField("id", id).WithField("error", err).Warn("Unable to delete container")
	//		}
	//	}
	//}()

	wg.Wait()
}
