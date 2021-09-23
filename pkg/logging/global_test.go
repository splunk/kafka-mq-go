package logging_test

import (
	"bytes"

	. "github.com/splunk/kafka-mq-go/pkg/logging"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Global", func() {
	Describe("NewGlobal", func() {
		It("should create a new global logger", func() {
			pre := Global().Logger()
			now := NewGlobal("logging-test").Logger()
			Expect(now).Should(Equal(Global().Logger()))
			Expect(now).ShouldNot(Equal(pre))
		})
	})

	Describe("SetGlobalLogger", func() {
		It("should replace the global logger", func() {
			pre := Global()
			now := New("test")

			SetGlobalLogger(now)
			Expect(Global()).Should(Equal(now))
			Expect(Global()).ShouldNot(Equal(pre))
		})
	})

	Describe("With new fields", func() {
		It("should create a new logger", func() {
			pre := Global()
			newLogger := pre.WithComponent("test")
			Expect(pre).ShouldNot(Equal(newLogger))
		})
	})

	Describe("NewWithOutput", func() {
		It("should create a new logger with a customized output", func() {
			buffer := new(bytes.Buffer)
			logger := NewWithOutput("test", buffer)
			logger.Info("logsomething")

			Expect(buffer.String()).Should(ContainSubstring("logsomething"))
		})
	})
})
