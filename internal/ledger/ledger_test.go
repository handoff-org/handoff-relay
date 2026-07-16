package ledger

import (
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T) *Ledger {
	t.Helper()
	l, err := Open(filepath.Join(t.TempDir(), "ledger.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l
}

func TestBalance_NewAccountGetsBonusOnce(t *testing.T) {
	l := openTemp(t)
	bal, earned, spent, err := l.Balance("hash-a")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if bal != SIGNUP_BONUS {
		t.Errorf("balance = %d, want %d", bal, SIGNUP_BONUS)
	}
	if earned != SIGNUP_BONUS {
		t.Errorf("earned = %d, want %d", earned, SIGNUP_BONUS)
	}
	if spent != 0 {
		t.Errorf("spent = %d, want 0", spent)
	}

	// Second call should not add another bonus.
	bal2, _, _, err := l.Balance("hash-a")
	if err != nil {
		t.Fatalf("Balance (2nd): %v", err)
	}
	if bal2 != SIGNUP_BONUS {
		t.Errorf("balance after 2nd call = %d, want %d", bal2, SIGNUP_BONUS)
	}
}

func TestSettle_TransfersTokens(t *testing.T) {
	l := openTemp(t)

	// Seed consumer with a bonus account.
	_, _, _, _ = l.Balance("consumer")
	_, _, _, _ = l.Balance("provider")

	if err := l.Settle("job-1", "consumer", "provider", 1000); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	cBal, _, cSpent, err := l.Balance("consumer")
	if err != nil {
		t.Fatalf("Balance consumer: %v", err)
	}
	if cBal != SIGNUP_BONUS-1000 {
		t.Errorf("consumer balance = %d, want %d", cBal, SIGNUP_BONUS-1000)
	}
	if cSpent != 1000 {
		t.Errorf("consumer spent = %d, want 1000", cSpent)
	}

	pBal, pEarned, _, err := l.Balance("provider")
	if err != nil {
		t.Fatalf("Balance provider: %v", err)
	}
	if pBal != SIGNUP_BONUS+1000 {
		t.Errorf("provider balance = %d, want %d", pBal, SIGNUP_BONUS+1000)
	}
	if pEarned != SIGNUP_BONUS+1000 {
		t.Errorf("provider earned = %d, want %d", pEarned, SIGNUP_BONUS+1000)
	}
}

func TestSettle_IsAtomic(t *testing.T) {
	l := openTemp(t)

	// Settling the same job ID twice should fail (PRIMARY KEY constraint) and
	// leave balances unchanged from the first Settle.
	_, _, _, _ = l.Balance("c")
	_, _, _, _ = l.Balance("p")

	if err := l.Settle("job-dup", "c", "p", 500); err != nil {
		t.Fatalf("first Settle: %v", err)
	}
	if err := l.Settle("job-dup", "c", "p", 500); err == nil {
		t.Error("second Settle with same jobID should fail, got nil")
	}

	// Balance should still reflect only the first settlement.
	bal, _, spent, _ := l.Balance("c")
	if bal != SIGNUP_BONUS-500 {
		t.Errorf("consumer balance = %d after duplicate settle, want %d", bal, SIGNUP_BONUS-500)
	}
	if spent != 500 {
		t.Errorf("consumer spent = %d after duplicate settle, want 500", spent)
	}
}

func TestRate_UpdatesProviderRating(t *testing.T) {
	l := openTemp(t)
	_, _, _, _ = l.Balance("c-rate")
	_, _, _, _ = l.Balance("p-rate")
	_ = l.Settle("job-r", "c-rate", "p-rate", 100)

	if err := l.Rate("job-r", 5); err != nil {
		t.Fatalf("Rate: %v", err)
	}
}

func TestRate_UnknownJobIsNoOp(t *testing.T) {
	l := openTemp(t)
	// Rating a nonexistent job should not error.
	if err := l.Rate("no-such-job", 3); err != nil {
		t.Errorf("Rate unknown job: %v", err)
	}
}

func TestSuspendCheck_NotSuspendedWithFewRatings(t *testing.T) {
	l := openTemp(t)
	_, _, _, _ = l.Balance("c")
	_, _, _, _ = l.Balance("p-ok")
	_ = l.Settle("j1", "c", "p-ok", 1)
	_ = l.Settle("j2", "c", "p-ok", 1)
	_ = l.Rate("j1", 1)
	_ = l.Rate("j2", 1) // only 2 ratings, need 3 to trigger

	susp, err := l.SuspendCheck("p-ok")
	if err != nil {
		t.Fatalf("SuspendCheck: %v", err)
	}
	if susp {
		t.Error("provider with only 2 bad ratings should not be suspended")
	}
}

func TestSuspendCheck_SuspendsOnThreeLowRatings(t *testing.T) {
	l := openTemp(t)
	_, _, _, _ = l.Balance("c")
	_, _, _, _ = l.Balance("p-bad")
	for i, id := range []string{"b1", "b2", "b3"} {
		_ = l.Settle(id, "c", "p-bad", int64(i+1))
		_ = l.Rate(id, 1) // rating 1 each time
	}

	susp, err := l.SuspendCheck("p-bad")
	if err != nil {
		t.Fatalf("SuspendCheck: %v", err)
	}
	if !susp {
		t.Error("provider with 3 ratings of 1 should be suspended")
	}
}

func TestSuspendCheck_NotSuspendedWithHighRatings(t *testing.T) {
	l := openTemp(t)
	_, _, _, _ = l.Balance("c")
	_, _, _, _ = l.Balance("p-good")
	for i, id := range []string{"g1", "g2", "g3"} {
		_ = l.Settle(id, "c", "p-good", int64(i+1))
		_ = l.Rate(id, 5)
	}

	susp, err := l.SuspendCheck("p-good")
	if err != nil {
		t.Fatalf("SuspendCheck: %v", err)
	}
	if susp {
		t.Error("provider with 3 ratings of 5 should not be suspended")
	}
}

func TestSettle_RefusesOverdraft(t *testing.T) {
	l := openTemp(t)
	_, _, _, _ = l.Balance("c-od")
	_, _, _, _ = l.Balance("p-od")

	// Drain consumer almost to zero.
	_ = l.Settle("drain", "c-od", "p-od", SIGNUP_BONUS-1)

	// This settle should fail: consumer only has 1 credit left, not 100.
	if err := l.Settle("overdraft", "c-od", "p-od", 100); err == nil {
		t.Error("Settle should fail when consumer balance < tokens")
	}

	// Balance should be unchanged from the last successful settle.
	bal, _, _, _ := l.Balance("c-od")
	if bal != 1 {
		t.Errorf("consumer balance = %d after refused overdraft, want 1", bal)
	}
}

func TestJobConsumer_ReturnsConsumerHash(t *testing.T) {
	l := openTemp(t)
	_, _, _, _ = l.Balance("c-jc")
	_, _, _, _ = l.Balance("p-jc")
	_ = l.Settle("job-consumer-test", "c-jc", "p-jc", 10)

	h, err := l.JobConsumer("job-consumer-test")
	if err != nil {
		t.Fatalf("JobConsumer: %v", err)
	}
	if h != "c-jc" {
		t.Errorf("JobConsumer = %q, want c-jc", h)
	}
}

func TestJobConsumer_ReturnsEmptyForUnknownJob(t *testing.T) {
	l := openTemp(t)
	h, err := l.JobConsumer("nonexistent-job")
	if err != nil {
		t.Fatalf("JobConsumer error: %v", err)
	}
	if h != "" {
		t.Errorf("JobConsumer = %q for unknown job, want empty string", h)
	}
}
