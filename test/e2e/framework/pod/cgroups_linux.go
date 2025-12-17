/*
Copyright 2025 The Kubernetes Authors.

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

package pod

import (
	"fmt"
	"math"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kubecm "k8s.io/kubernetes/pkg/kubelet/cm"
)

func GetExpectedCPUShares(rr *v1.ResourceRequirements, podOnCgroupv2 bool) []string {
	// This function is moved out from cgroups.go because opencontainers/cgroups can only be compiled in linux platforms.
	cpuRequest := rr.Requests.Cpu()
	cpuLimit := rr.Limits.Cpu()
	var shares int64
	if cpuRequest.IsZero() && !cpuLimit.IsZero() {
		shares = int64(kubecm.MilliCPUToShares(cpuLimit.MilliValue()))
	} else {
		shares = int64(kubecm.MilliCPUToShares(cpuRequest.MilliValue()))
	}
	if podOnCgroupv2 {
		// Because of https://github.com/kubernetes/kubernetes/issues/131216, the way of conversion has been changed.
		// runc: https://github.com/opencontainers/runc/pull/4785
		// crun: https://github.com/containers/crun/issues/1721
		// This is dependent on the container runtime version. In order not to break the tests when we upgrade the
		// container runtimes, we check if either the old or the new conversion matches the actual value for now.
		// TODO: Remove the old conversion once container runtimes are updated.
		oldConverted := 1 + ((shares-2)*9999)/262142
		converted := ConvertCPUSharesToCgroupV2Value(uint64(shares))
		return []string{strconv.FormatInt(oldConverted, 10), strconv.FormatInt(int64(converted), 10)}
	} else {
		return []string{strconv.FormatInt(shares, 10)}
	}
}

// ConvertCPUSharesToCgroupV2Value matches opencontainers/cgroups, but avoids pulling it as a dependency.
func ConvertCPUSharesToCgroupV2Value(cpuShares uint64) uint64 {
	// The value of 0 means "unset".
	if cpuShares == 0 {
		return 0
	}
	if cpuShares <= 2 {
		return 1
	}
	if cpuShares >= 262144 {
		return 10000
	}
	l := math.Log2(float64(cpuShares))
	// Quadratic function which fits min, max, and default.
	exponent := (l*l+125*l)/612.0 - 7.0/34.0

	return uint64(math.Ceil(math.Pow(10, exponent)))
}

// TODO: Remove the rounded cpu limit values when https://github.com/opencontainers/runc/issues/4622
// is fixed.
func GetCPULimitCgroupExpectations(cpuLimit *resource.Quantity) []string {
	var expectedCPULimits []string
	milliCPULimit := cpuLimit.MilliValue()

	cpuQuota := kubecm.MilliCPUToQuota(milliCPULimit, kubecm.QuotaPeriod)
	if cpuLimit.IsZero() {
		cpuQuota = -1
	}
	expectedCPULimits = append(expectedCPULimits, getExpectedCPULimitFromCPUQuota(cpuQuota))

	if milliCPULimit%10 != 0 && cpuQuota != -1 {
		roundedCPULimit := (milliCPULimit/10 + 1) * 10
		cpuQuotaRounded := kubecm.MilliCPUToQuota(roundedCPULimit, kubecm.QuotaPeriod)
		expectedCPULimits = append(expectedCPULimits, getExpectedCPULimitFromCPUQuota(cpuQuotaRounded))
	}

	return expectedCPULimits
}

func getExpectedCPULimitFromCPUQuota(cpuQuota int64) string {
	expectedCPULimitString := strconv.FormatInt(cpuQuota, 10)
	if *podOnCgroupv2Node {
		if expectedCPULimitString == "-1" {
			expectedCPULimitString = "max"
		}
		expectedCPULimitString = fmt.Sprintf("%s %s", expectedCPULimitString, CPUPeriod)
	}
	return expectedCPULimitString
}
