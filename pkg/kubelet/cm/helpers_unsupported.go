//go:build !linux
// +build !linux

/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cm

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kubefeatures "k8s.io/kubernetes/pkg/features"
)

const (
	// These limits are defined in the kernel:
	// https://github.com/torvalds/linux/blob/0bddd227f3dc55975e2b8dfa7fc6f959b062a2c7/kernel/sched/sched.h#L427-L428
	MinShares = 2
	MaxShares = 262144

	SharesPerCPU  = 1024
	MilliCPUToCPU = 1000

	// 100000 microseconds is equivalent to 100ms
	QuotaPeriod = 100000
	// 1000 microseconds is equivalent to 1ms
	// defined here:
	// https://github.com/torvalds/linux/blob/cac03ac368fabff0122853de2422d4e17a32de08/kernel/sched/core.c#L10546
	MinQuotaPeriod = 1000

	// From the inverse of the conversion in MilliCPUToQuota:
	// MinQuotaPeriod * MilliCPUToCPU / QuotaPeriod
	MinMilliCPULimit = 10
)

// MilliCPUToQuota converts milliCPU to CFS quota and period values.
// Input parameters and resulting value is number of microseconds.
func MilliCPUToQuota(milliCPU int64, period int64) (quota int64) {
	// CFS quota is measured in two values:
	//  - cfs_period_us=100ms (the amount of time to measure usage across given by period)
	//  - cfs_quota=20ms (the amount of cpu time allowed to be used across a period)
	// so in the above example, you are limited to 20% of a single CPU
	// for multi-cpu environments, you just scale equivalent amounts
	// see https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt for details

	if milliCPU == 0 {
		return
	}

	if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CPUCFSQuotaPeriod) {
		period = QuotaPeriod
	}

	// we then convert your milliCPU to a value normalized over a period
	quota = (milliCPU * period) / MilliCPUToCPU

	// quota needs to be a minimum of 1ms.
	if quota < MinQuotaPeriod {
		quota = MinQuotaPeriod
	}
	return
}

// MilliCPUToShares converts the milliCPU to CFS shares.
func MilliCPUToShares(milliCPU int64) uint64 {
	if milliCPU == 0 {
		// Docker converts zero milliCPU to unset, which maps to kernel default
		// for unset: 1024. Return 2 here to really match kernel default for
		// zero milliCPU.
		return MinShares
	}
	// Conceptually (milliCPU / milliCPUToCPU) * sharesPerCPU, but factored to improve rounding.
	shares := (milliCPU * SharesPerCPU) / MilliCPUToCPU
	if shares < MinShares {
		return MinShares
	}
	if shares > MaxShares {
		return MaxShares
	}
	return uint64(shares)
}

// ResourceConfigForPod takes the input pod and outputs the cgroup resource config.
func ResourceConfigForPod(pod *v1.Pod, enforceCPULimit bool, cpuPeriod uint64, enforceMemoryQoS bool) *ResourceConfig {
	return nil
}

// GetCgroupSubsystems returns information about the mounted cgroup subsystems
func GetCgroupSubsystems() (*CgroupSubsystems, error) {
	return nil, nil
}

func getCgroupProcs(dir string) ([]int, error) {
	return nil, nil
}

// GetPodCgroupNameSuffix returns the last element of the pod CgroupName identifier
func GetPodCgroupNameSuffix(podUID types.UID) string {
	return ""
}

// NodeAllocatableRoot returns the literal cgroup path for the node allocatable cgroup
func NodeAllocatableRoot(cgroupRoot string, cgroupsPerQOS bool, cgroupDriver string) string {
	return ""
}

// GetKubeletContainer returns the cgroup the kubelet will use
func GetKubeletContainer(kubeletCgroups string) (string, error) {
	return "", nil
}
