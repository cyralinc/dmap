package classification

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMerge_WhenCalledOnNilReceiver_ShouldNotPanic(t *testing.T) {
	var result Result
	require.NotPanics(
		t, func() {
			result.Merge(Result{"age": {"AGE": {Name: "AGE"}}})
		},
	)
}

func TestMerge_WhenCalledWithNonExistingAttributes_ShouldAddThem(t *testing.T) {
	result := Result{
		"age": {"AGE": {Name: "AGE"}},
	}
	other := Result{
		"social_sec_num": {"SSN": {Name: "SSN"}},
	}
	expected := Result{
		"age":            {"AGE": {Name: "AGE"}},
		"social_sec_num": {"SSN": {Name: "SSN"}},
	}
	result.Merge(other)
	require.Equal(t, expected, result)
}

func TestMerge_WhenCalledWithExistingAttributes_ShouldMergeLabelSets(t *testing.T) {
	result := Result{
		"age": {"AGE": {Name: "AGE"}},
	}
	other := Result{
		"age": {"CVV": {Name: "CVV"}},
	}
	expected := Result{
		"age": {"AGE": {Name: "AGE"}, "CVV": {Name: "CVV"}},
	}
	result.Merge(other)
	require.Equal(t, expected, result)
}

func TestMerge_WhenCalledWithExistingAttributes_ShouldOverwrite(t *testing.T) {
	result := Result{
		"age": {"AGE": {Name: "AGE", Description: "Foo"}},
	}
	other := Result{
		"age": {"AGE": {Name: "AGE", Description: "Bar"}},
	}
	expected := Result{
		"age": {"AGE": {Name: "AGE", Description: "Bar"}},
	}
	result.Merge(other)
	require.Equal(t, expected, result)
}
