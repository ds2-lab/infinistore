package promise

import (
	"runtime"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	ret = &struct{}{}
)

func TestTypes(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Promise")
}

func shouldNotTimeout(test func() interface{}, expects ...bool) interface{} {
	expect := false
	if len(expects) > 0 {
		expect = expects[0]
	}

	timer := time.NewTimer(time.Second)
	timeout := false
	responeded := make(chan interface{})
	var ret interface{}
	go func() {
		responeded <- test()
	}()
	select {
	case <-timer.C:
		timeout = true
	case ret = <-responeded:
		if !timer.Stop() {
			<-timer.C
		}
	}

	Expect(timeout).To(Equal(expect))
	return ret
}

func shouldTimeout(test func() interface{}) {
	shouldNotTimeout(test, true)
}

var _ = Describe("ChannelPromise", func() {
	It("no wait if data has been available already", func() {
		promise := NewChannelPromise()
		promise.Resolve(ret)

		Expect(shouldNotTimeout(promise.Value)).To(Equal(ret))
	})

	It("should wait if data is not available", func() {
		promise := NewChannelPromise()

		shouldTimeout(promise.Value)
	})

	It("should wait if data is not available", func() {
		promise := NewChannelPromise()

		var done sync.WaitGroup
		done.Add(1)
		go func() {
			Expect(shouldNotTimeout(promise.Value)).To(Equal(ret))
			done.Done()
		}()
		runtime.Gosched()

		<-time.After(500 * time.Millisecond)
		promise.Resolve(ret)

		done.Wait()
	})

	It("should timeout as expected", func() {
		promise := NewChannelPromise()
		promise.SetTimeout(100 * time.Millisecond)

		var done sync.WaitGroup
		done.Add(1)
		go func() {
			shouldTimeout(promise.Value)
			done.Done()
		}()
		runtime.Gosched()

		Expect(shouldNotTimeout(func() interface{} {
			return promise.Timeout()
		})).To(Equal(ErrTimeout))

		done.Wait()
	})

	It("should not timeout if value has been available", func() {
		promise := NewChannelPromise()
		promise.Resolve(ret, nil)
		promise.SetTimeout(2000 * time.Millisecond)

		Expect(shouldNotTimeout(func() interface{} {
			return promise.Timeout()
		})).To(BeNil())

		Expect(shouldNotTimeout(promise.Value)).To(Equal(ret))
		Expect(promise.Error()).To(BeNil())
	})

	It("should not timeout if value is available", func() {
		promise := NewChannelPromise()
		promise.SetTimeout(100 * time.Millisecond)

		var done sync.WaitGroup
		done.Add(1)
		go func() {
			promise.Resolve(ret, nil)
			done.Done()
		}()

		Expect(shouldNotTimeout(func() interface{} {
			return promise.Timeout()
		})).To(BeNil())

		Expect(shouldNotTimeout(promise.Value)).To(Equal(ret))
		Expect(promise.Error()).To(BeNil())

		done.Wait()
	})

	It("should not timeout multiple times", func() {
		promise := NewChannelPromise()

		var done sync.WaitGroup
		done.Add(1)
		go func() {
			<-time.After(250 * time.Millisecond)
			promise.Resolve(ret, nil)
			done.Done()
		}()

		promise.SetTimeout(100 * time.Millisecond)
		Expect(shouldNotTimeout(func() interface{} {
			return promise.Timeout()
		})).To(Equal(ErrTimeout))

		promise.SetTimeout(100 * time.Millisecond)
		Expect(shouldNotTimeout(func() interface{} {
			return promise.Timeout()
		})).To(Equal(ErrTimeout))

		promise.SetTimeout(100 * time.Millisecond)
		Expect(shouldNotTimeout(func() interface{} {
			return promise.Timeout()
		})).To(BeNil())

		Expect(shouldNotTimeout(promise.Value)).To(Equal(ret))
		Expect(promise.Error()).To(BeNil())

		done.Wait()
	})
})

func BenchmarkNewChannel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		promise := NewChannelPromise()
		promise.Close()
	}
}

func BenchmarkNewPromise(b *testing.B) {
	for i := 0; i < b.N; i++ {
		promise := NewPromise()
		promise.Close()
	}
}

func BenchmarkChannelResolvedCheck(b *testing.B) {
	for i := 0; i < b.N; i++ {
		promise := NewChannelPromise()
		if !promise.IsResolved() {
			promise.Close()
		}
	}
}

func BenchmarkPromiseResolvedCheck(b *testing.B) {
	for i := 0; i < b.N; i++ {
		promise := NewPromise()
		if !promise.IsResolved() {
			promise.Close()
		}
	}
}

func BenchmarkChannelNotification(b *testing.B) {
	for i := 0; i < b.N; i++ {
		promise := NewChannelPromise()
		go func() {
			promise.Resolve(ret)
		}()
		promise.Value()
	}
}

func BenchmarkPromiseNotification(b *testing.B) {
	for i := 0; i < b.N; i++ {
		promise := NewPromise()
		go func() {
			promise.Resolve(ret)
		}()
		promise.Value()
	}
}
