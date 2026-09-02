/*
Copyright 2026 The Kubernetes Authors.

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

package metrics

import (
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/utils/ptr"
)

func controlPlanePodObject(component, node string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      component + "-" + node,
			Namespace: metav1.NamespaceSystem,
		},
		Spec: v1.PodSpec{NodeName: node},
	}
}

func leaseObject(name, holder string) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceSystem,
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr.To(holder)},
	}
}

// TestNewMetricsGrabberFindsAllReplicas covers the case that used to break on
// clusters with more than one control plane node: every replica has to be
// remembered, because the grabber cannot tell yet which one is the leader.
func TestNewMetricsGrabberFindsAllReplicas(t *testing.T) {
	var objects []runtime.Object
	for _, node := range []string{"cp-1", "cp-2", "cp-3"} {
		objects = append(objects,
			controlPlanePodObject("kube-controller-manager", node),
			controlPlanePodObject("kube-scheduler", node),
		)
	}
	client := fake.NewSimpleClientset(objects...)

	grabber, err := NewMetricsGrabber(t.Context(), client, nil, &rest.Config{}, false, true, true, false, false, false)
	if err != nil {
		t.Fatalf("NewMetricsGrabber failed: %v", err)
	}
	if !grabber.HasControlPlanePods() {
		t.Error("HasControlPlanePods() = false, want true")
	}
	for _, tc := range []struct {
		component string
		pods      []controlPlanePod
	}{
		{"kube-controller-manager", grabber.kubeControllerManagers},
		{"kube-scheduler", grabber.kubeSchedulers},
	} {
		if len(tc.pods) != 3 {
			t.Errorf("found %d %s replicas (%v), want 3", len(tc.pods), tc.component, tc.pods)
		}
	}
}

func TestLeaderFirst(t *testing.T) {
	pods := []controlPlanePod{
		{name: "kube-controller-manager-cp-1", nodeName: "cp-1.example.com"},
		{name: "kube-controller-manager-cp-2", nodeName: "cp-2.example.com"},
		{name: "kube-controller-manager-cp-3", nodeName: "cp-3.example.com"},
	}

	testcases := map[string]struct {
		lease *coordinationv1.Lease
		want  string
	}{
		"no lease": {
			want: "kube-controller-manager-cp-1",
		},
		"holder matches the node name": {
			lease: leaseObject(kubeControllerManagerLease, "cp-3.example.com_1a2b3c4d-0000-0000-0000-000000000000"),
			want:  "kube-controller-manager-cp-3",
		},
		"holder is the unqualified hostname": {
			lease: leaseObject(kubeControllerManagerLease, "cp-2_1a2b3c4d-0000-0000-0000-000000000000"),
			want:  "kube-controller-manager-cp-2",
		},
		// kOps on DigitalOcean names nodes after the droplet's private IP, so
		// the hostname in the lease matches none of them. Ordering then falls
		// back to list order and grabFromLeader has to sort it out.
		"holder matches no node": {
			lease: leaseObject(kubeControllerManagerLease, "control-plane-lon1-1_1a2b3c4d-0000-0000-0000-000000000000"),
			want:  "kube-controller-manager-cp-1",
		},
		"holder without a uniquifier": {
			lease: leaseObject(kubeControllerManagerLease, "cp-3.example.com"),
			want:  "kube-controller-manager-cp-3",
		},
		"empty holder": {
			lease: leaseObject(kubeControllerManagerLease, ""),
			want:  "kube-controller-manager-cp-1",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			var objects []runtime.Object
			if tc.lease != nil {
				objects = append(objects, tc.lease)
			}
			client := fake.NewSimpleClientset(objects...)

			ordered := leaderFirst(t.Context(), client, kubeControllerManagerLease, pods)
			if ordered[0].name != tc.want {
				t.Errorf("first replica is %q, want %q", ordered[0].name, tc.want)
			}
			if len(ordered) != len(pods) {
				t.Errorf("got %d replicas, want %d", len(ordered), len(pods))
			}
			if pods[0].name != "kube-controller-manager-cp-1" {
				t.Error("leaderFirst reordered the slice it was given")
			}
		})
	}
}

func TestSameHost(t *testing.T) {
	testcases := []struct {
		nodeName string
		hostname string
		want     bool
	}{
		{"cp-1", "cp-1", true},
		{"CP-1", "cp-1", true},
		{"cp-1.example.com", "cp-1", true},
		{"cp-1", "cp-1.example.com", true},
		{"cp-1", "cp-2", false},
		{"cp-11", "cp-1", false},
		{"10.106.0.6", "control-plane-lon1-1", false},
		{"10.106.0.6", "10.106.0.7", false},
		{"", "cp-1", false},
		{"cp-1", "", false},
	}

	for _, tc := range testcases {
		if got := sameHost(tc.nodeName, tc.hostname); got != tc.want {
			t.Errorf("sameHost(%q, %q) = %v, want %v", tc.nodeName, tc.hostname, got, tc.want)
		}
	}
}

// leaderElectionStatusData wraps a sample in the exposition format that
// testutil.ParseMetrics expects. Note that it loops forever on input it cannot
// parse, so the type hint and the trailing newline are not optional.
func leaderElectionStatusData(sample string) string {
	return "# TYPE " + leaderElectionStatusMetric + " gauge\n" + sample + "\n"
}

func TestHoldsLease(t *testing.T) {
	testcases := map[string]struct {
		data         string
		wantHeld     bool
		wantExported bool
	}{
		"leader": {
			data:         leaderElectionStatusData(`leader_election_master_status{name="kube-controller-manager"} 1`),
			wantHeld:     true,
			wantExported: true,
		},
		// A standby reports the metric as 0: client-go sets it as soon as the
		// leader elector is constructed, well before any election happens.
		"standby": {
			data:         leaderElectionStatusData(`leader_election_master_status{name="kube-controller-manager"} 0`),
			wantExported: true,
		},
		// Components started with --leader-elect=false never construct a leader
		// elector, and so never register the metric.
		"leader election disabled": {
			data: "# TYPE process_start_time_seconds gauge\nprocess_start_time_seconds 1.7e+09\n",
		},
		"other lease only": {
			data: leaderElectionStatusData(`leader_election_master_status{name="kube-scheduler"} 1`),
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			metrics := testutil.NewMetrics()
			if err := testutil.ParseMetrics(tc.data, &metrics); err != nil {
				t.Fatalf("ParseMetrics failed: %v", err)
			}
			held, exported := holdsLease(metrics, kubeControllerManagerLease)
			if held != tc.wantHeld || exported != tc.wantExported {
				t.Errorf("holdsLease() = (%v, %v), want (%v, %v)", held, exported, tc.wantHeld, tc.wantExported)
			}
		})
	}
}
