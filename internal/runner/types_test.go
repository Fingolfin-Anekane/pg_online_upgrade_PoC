package runner_test

import (
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/stretchr/testify/assert"
)

func TestStepStatus_Values(t *testing.T) {
	assert.Equal(t, runner.StepStatus("pending"), runner.StepPending)
	assert.Equal(t, runner.StepStatus("skipped"), runner.StepSkipped)
	assert.Equal(t, runner.StepStatus("running"), runner.StepRunning)
	assert.Equal(t, runner.StepStatus("done"), runner.StepDone)
	assert.Equal(t, runner.StepStatus("failed"), runner.StepFailed)
}

func TestRunMode_Distinct(t *testing.T) {
	assert.NotEqual(t, runner.Interactive, runner.Headless)
}
