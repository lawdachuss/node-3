package coordinator

import (
	"strings"
	"testing"

	"github.com/teacat/chaturbate-dvr/database"
)

// Task B2 tests. The manager seam (ReleaseHook) is pinned in
// manager/zero_byte_release_test.go; these pin the coordinator side it is
// wired to: only THIS node's own fresh 'recording' rows are reset to
// 'claimed' — ReclaimChannels skips 'recording' rows, and the controller's
// stale-marker reset deliberately spares protected (alive) nodes, so without
// an owner-side release the cleared wedge burns the slot until the node
// restarts (2026-10-17 sweep: 12/11/8 ghost slots on node-4/8/16).

func TestDeadRunReleaseResetsOwnRecordingRow(t *testing.T) {
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{},
		singleRow: []database.ChannelAssignment{
			{Username: "diana", Site: "chaturbate", AssignedNode: "node-a", Status: "recording"},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["diana"] = &syncChannelState{site: "chaturbate", recording: false}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client, recordingMarked: map[recMarkKey]bool{}}
	coord.recordingMarked[recMarkKey{site: "chaturbate", username: "diana"}] = true

	coord.runDeadRunReleaseWith("diana", client)

	rec := fake.record("POST", "/rpc/reset_channel_status", `"p_status":"claimed"`)
	if rec == nil {
		t.Fatalf("own stale 'recording' row was never reset to claimed (reqs=%v)", fake.reqs)
	}
	if !strings.Contains(rec.body, `"p_username":"diana"`) {
		t.Errorf("reset targeted the wrong channel: %s", rec.body)
	}
	if coord.recordingMarked[recMarkKey{site: "chaturbate", username: "diana"}] {
		t.Errorf("heartbeat marker was kept — the next mark phase must not re-mark a released row")
	}
}

func TestDeadRunReleaseSkipsRowMovedToAnotherNode(t *testing.T) {
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{},
		singleRow: []database.ChannelAssignment{
			{Username: "erin", Site: "chaturbate", AssignedNode: "node-b", Status: "recording"},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["erin"] = &syncChannelState{site: "chaturbate", recording: false}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client, recordingMarked: map[recMarkKey]bool{}}
	coord.recordingMarked[recMarkKey{site: "chaturbate", username: "erin"}] = true

	coord.runDeadRunReleaseWith("erin", client)

	if rec := fake.record("POST", "/rpc/reset_channel_status", ""); rec != nil {
		t.Errorf("released someone else's row: %s %s", rec.method, rec.body)
	}
}

func TestDeadRunReleaseSkipsChannelRecordingAgain(t *testing.T) {
	// The stream came back between the dead-run clear and the release: the row
	// is genuinely live again and the mark phase is re-asserting it. Resetting
	// it to 'claimed' would un-pin a real capture.
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{},
		singleRow: []database.ChannelAssignment{
			{Username: "freya", Site: "chaturbate", AssignedNode: "node-a", Status: "recording"},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["freya"] = &syncChannelState{site: "chaturbate", recording: true}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client, recordingMarked: map[recMarkKey]bool{}}
	coord.recordingMarked[recMarkKey{site: "chaturbate", username: "freya"}] = true

	coord.runDeadRunReleaseWith("freya", client)

	if rec := fake.record("POST", "/rpc/reset_channel_status", ""); rec != nil {
		t.Errorf("un-pinned a capture that went live again: %s %s", rec.method, rec.body)
	}
}

func TestDeadRunReleaseSkipsAlreadyClaimedRow(t *testing.T) {
	// The sync loop's idle phase already reset this row; the release is late.
	// Resetting again is pointless churn — but the marker still must go.
	fake := &fakeSyncDB{
		nodeAssignments: []database.ChannelAssignment{},
		singleRow: []database.ChannelAssignment{
			{Username: "gina", Site: "chaturbate", AssignedNode: "node-a", Status: "claimed"},
		},
	}
	client := newFakeSyncClient(t, fake)

	stub := newSyncChanStub()
	stub.local["gina"] = &syncChannelState{site: "chaturbate", recording: false}

	coord := &Coordinator{NodeID: "node-a", Manager: stub, Client: client, recordingMarked: map[recMarkKey]bool{}}
	coord.recordingMarked[recMarkKey{site: "chaturbate", username: "gina"}] = true

	coord.runDeadRunReleaseWith("gina", client)

	if rec := fake.record("POST", "/rpc/reset_channel_status", ""); rec != nil {
		t.Errorf("reset an already-claimed row: %s %s", rec.method, rec.body)
	}
	if coord.recordingMarked[recMarkKey{site: "chaturbate", username: "gina"}] {
		t.Errorf("stale heartbeat marker survived the release")
	}
}
