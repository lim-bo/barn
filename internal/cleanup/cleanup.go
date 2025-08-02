package cleanup

import "log"

var (
	jobs = make([]*Job, 0)
)

type Job struct {
	Name string
	Func func() error
}

func Register(j *Job) {
	jobs = append(jobs, j)
}

func CleanUp() {
	log.Println("cleaning up resources...")
	var err error
	for _, j := range jobs {
		log.Printf("running %s", j.Name)
		err = j.Func()
		if err != nil {
			log.Printf("error cleaning: %s", err.Error())
		}
	}
	log.Println("cleanup done")
}
