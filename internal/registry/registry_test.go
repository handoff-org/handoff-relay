package registry

import (
	"testing"
)

// fakeProvider returns a Provider with a nil Conn — safe for testing Register/Unregister/Pick
// since those methods never call Conn.WriteJSON.
func fakeProvider(id string, models ...string) *Provider {
	return &Provider{ID: id, Models: models}
}

func TestRegister_PickFindsProvider(t *testing.T) {
	r := New()
	r.Register(fakeProvider("p1", "llama3"))

	p := r.Pick("llama3")
	if p == nil {
		t.Fatal("Pick returned nil for a registered model")
	}
	if p.ID != "p1" {
		t.Errorf("Pick returned %q, want p1", p.ID)
	}
}

func TestPick_ReturnsNilForUnknownModel(t *testing.T) {
	r := New()
	r.Register(fakeProvider("p1", "llama3"))
	if got := r.Pick("unknown-model"); got != nil {
		t.Errorf("Pick(unknown) = %v, want nil", got)
	}
}

func TestUnregister_RemovesProvider(t *testing.T) {
	r := New()
	r.Register(fakeProvider("p1", "llama3"))
	r.Unregister("p1")

	if got := r.Pick("llama3"); got != nil {
		t.Errorf("Pick after Unregister = %v, want nil", got)
	}
}

func TestUnregister_UnknownIDIsNoOp(t *testing.T) {
	r := New()
	r.Register(fakeProvider("p1", "llama3"))
	r.Unregister("does-not-exist") // should not panic or corrupt state

	if got := r.Pick("llama3"); got == nil {
		t.Error("Pick should still find p1 after unregistering an unknown id")
	}
}

func TestRegister_MultipleProvidersForSameModel(t *testing.T) {
	r := New()
	r.Register(fakeProvider("p1", "llama3"))
	r.Register(fakeProvider("p2", "llama3"))

	if r.Count("llama3") != 2 {
		t.Errorf("Count = %d, want 2", r.Count("llama3"))
	}
}

func TestUnregister_RemovesOnlyTargetProvider(t *testing.T) {
	r := New()
	r.Register(fakeProvider("p1", "llama3"))
	r.Register(fakeProvider("p2", "llama3"))
	r.Unregister("p1")

	if r.Count("llama3") != 1 {
		t.Errorf("Count after removing p1 = %d, want 1", r.Count("llama3"))
	}
	p := r.Pick("llama3")
	if p == nil || p.ID != "p2" {
		t.Errorf("Pick after removing p1 returned %v, want p2", p)
	}
}

func TestRegister_ProviderWithMultipleModels(t *testing.T) {
	r := New()
	r.Register(fakeProvider("p1", "llama3", "mistral"))

	if r.Pick("llama3") == nil {
		t.Error("Pick(llama3) should find p1")
	}
	if r.Pick("mistral") == nil {
		t.Error("Pick(mistral) should find p1")
	}
}

func TestUnregister_CleansUpAllModels(t *testing.T) {
	r := New()
	r.Register(fakeProvider("p1", "llama3", "mistral"))
	r.Unregister("p1")

	if r.Pick("llama3") != nil {
		t.Error("Pick(llama3) should be nil after unregistering multi-model provider")
	}
	if r.Pick("mistral") != nil {
		t.Error("Pick(mistral) should be nil after unregistering multi-model provider")
	}
}

func TestCount_ZeroForUnknownModel(t *testing.T) {
	r := New()
	if r.Count("no-model") != 0 {
		t.Error("Count for unknown model should be 0")
	}
}
