package admin_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestDeliveryLagSamplerStateTransitions(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 123).UTC()
	sampler := admin.NewDeliveryLagSamplerWithClock(func() time.Time { return start })
	full := admin.PublishDeliveryLagForTest(sampler, 1, mtproto.DeliveryLagSample{
		EligibleConnections: 2,
		SampledConnections:  2,
		WorstPts:            3,
	}, start)
	assertDeliveryLag(t, full, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 2, 2, 3, &start)

	partial := admin.PublishDeliveryLagForTest(sampler, 2, mtproto.DeliveryLagSample{
		EligibleConnections: 2,
		SampledConnections:  1,
		WorstPts:            99,
	}, start.Add(time.Second))
	assertDeliveryLag(t, partial, admin.DeliveryLagStale, admin.DeliveryLagCoveragePartial, 2, 1, 3, &start)

	none := admin.PublishDeliveryLagForTest(sampler, 3, mtproto.DeliveryLagSample{
		EligibleConnections: 2,
		SampledConnections:  0,
		WorstPts:            99,
	}, start.Add(2*time.Second))
	assertDeliveryLag(t, none, admin.DeliveryLagStale, admin.DeliveryLagCoverageNone, 2, 0, 3, &start)

	recoveredAt := start.Add(3 * time.Second)
	recovered := admin.PublishDeliveryLagForTest(sampler, 4, mtproto.DeliveryLagSample{
		EligibleConnections: 1,
		SampledConnections:  1,
		WorstPts:            0,
	}, recoveredAt)
	assertDeliveryLag(t, recovered, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 1, 1, 0, &recoveredAt)

	firstFailure := admin.NewDeliveryLagSamplerWithClock(func() time.Time { return start })
	unavailable := admin.PublishDeliveryLagForTest(firstFailure, 1, mtproto.DeliveryLagSample{
		EligibleConnections: 2,
		SampledConnections:  0,
	}, time.Time{})
	if unavailable.State != admin.DeliveryLagUnavailable || unavailable.Coverage != admin.DeliveryLagCoverageNone ||
		unavailable.WorstPts != nil || unavailable.SampledAt != nil {
		t.Fatalf("first failed sample = %+v, want unavailable with null value and time", unavailable)
	}

	empty := admin.NewDeliveryLagSamplerWithClock(func() time.Time { return start })
	zero := admin.PublishDeliveryLagForTest(empty, 1, mtproto.DeliveryLagSample{}, start)
	assertDeliveryLag(t, zero, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 0, 0, 0, &start)
}

func TestDeliveryLagJSONAndSSEHaveFixedObject(t *testing.T) {
	t.Parallel()

	sampledAt := time.Unix(1_700_000_000, 0).UTC()
	worstPts := int64(3)
	m := admin.MetricsResponse{DeliveryLag: admin.DeliveryLag{
		WorstPts:            &worstPts,
		State:               admin.DeliveryLagAvailable,
		Coverage:            admin.DeliveryLagCoverageFull,
		EligibleConnections: 2,
		SampledConnections:  2,
		SampledAt:           &sampledAt,
	}}

	encoded, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal metrics: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &root); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	jsonObject := decodeDeliveryLagObject(t, root["delivery_lag"])

	fragments, err := admin.DefaultFragmentRenderer(m)
	if err != nil {
		t.Fatalf("render SSE fragment: %v", err)
	}
	const open = `<script id="delivery-lag-telemetry" type="application/json">`
	start := strings.Index(fragments[0].HTML, open)
	if start < 0 {
		t.Fatal("SSE delivery-lag telemetry script missing")
	}
	start += len(open)
	end := strings.Index(fragments[0].HTML[start:], `</script>`)
	if end < 0 {
		t.Fatal("SSE delivery-lag telemetry script is not closed")
	}
	sseObject := decodeDeliveryLagObject(t, json.RawMessage(fragments[0].HTML[start:start+end]))
	if string(jsonObject["sampled_at"]) != string(sseObject["sampled_at"]) ||
		string(jsonObject["worst_pts"]) != string(sseObject["worst_pts"]) {
		t.Fatalf("SSE delivery lag = %v, JSON = %v", sseObject, jsonObject)
	}
}

func assertDeliveryLag(t *testing.T, got admin.DeliveryLag, state admin.DeliveryLagState, coverage admin.DeliveryLagCoverage, eligible, sampled int, worst int64, sampledAt *time.Time) {
	t.Helper()
	if got.State != state || got.Coverage != coverage || got.EligibleConnections != eligible || got.SampledConnections != sampled {
		t.Fatalf("delivery lag metadata = %+v, want state=%q coverage=%q eligible=%d sampled=%d", got, state, coverage, eligible, sampled)
	}
	if got.WorstPts == nil || *got.WorstPts != worst {
		t.Fatalf("delivery lag worst = %v, want %d", got.WorstPts, worst)
	}
	if got.SampledAt == nil || !got.SampledAt.Equal(*sampledAt) {
		t.Fatalf("delivery lag sampled_at = %v, want %v", got.SampledAt, sampledAt)
	}
}

func decodeDeliveryLagObject(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode delivery lag object: %v", err)
	}
	want := []string{"coverage", "eligible_connections", "sampled_at", "sampled_connections", "state", "worst_pts"}
	if got := sortedDeliveryLagKeys(object); !slices.Equal(got, want) {
		t.Fatalf("delivery lag keys = %v, want %v", got, want)
	}
	return object
}

func sortedDeliveryLagKeys(object map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
