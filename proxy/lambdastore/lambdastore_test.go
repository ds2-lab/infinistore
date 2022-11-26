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
	timeout := false
	responeded := make(chan struct{})
	timer := time.NewTimer(time.Second)
	go func() {
		test()
		responeded <- struct{}{}
	}()
	select {
	case <-timer.C:
		timeout = true
	case <-responeded:
		timer.Stop()
	}

	switch e := expect.(type) {
	case bool:
		Expect(timeout).Should(Equal(e))
	case func(bool):
		e(timeout)
	}
}
