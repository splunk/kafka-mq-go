package matchers

import (
	"fmt"

	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/matchers"
	"github.com/onsi/gomega/types"
	"github.com/pkg/errors"
)

// BeCausedBy expects an error was caused by another error.
// The cause of the error is retrieved by using `"github.com/pkg/errors".Cause()`
func BeCausedBy(element interface{}) types.GomegaMatcher {
	return &causeMatcher{element}
}

type causeMatcher struct {
	element interface{}
}

func (matcher *causeMatcher) Match(actual interface{}) (success bool, err error) {
	elemMatcher, elementIsMatcher := matcher.element.(types.GomegaMatcher)
	if !elementIsMatcher {
		elemMatcher = &matchers.EqualMatcher{Expected: matcher.element}
	}

	actualErr, ok := actual.(error)
	if !ok {
		return false, fmt.Errorf("%v is not an error", actual)
	}
	return elemMatcher.Match(errors.Cause(actualErr))
}

func (matcher *causeMatcher) FailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "to be caused by", matcher.element)
}

func (matcher *causeMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "not to be caused by", matcher.element)
}
