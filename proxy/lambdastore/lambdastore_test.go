package lambdastore

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLambdastore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lambdastore")
}

func shouldTimeout(test func(), expect interface{}) {
	timer := time.NewTimer(time.Second)
	timeout := false
	responeded := make(chan struct{})
	go func() {
		test()
		responeded <- struct{}{}
	}()
	select {
	case <-timer.C:
		timeout = true
	case <-responeded:
		if !timer.Stop() {
			<-timer.C
		}
	}

	switch e := expect.(type) {
	case bool:
		Expect(timeout).Should(Equal(e))
	case func(bool):
		e(timeout)
	}
}
