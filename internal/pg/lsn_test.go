package pg

import "testing"

func TestParseFormatRoundTrip(t *testing.T) {
	for _, s := range []string{"0/0", "0/3000060", "1/0", "16/B374D848", "FFFFFFFF/FFFFFFFF"} {
		l, err := ParseLSN(s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if got := l.String(); got != s {
			t.Fatalf("round trip %s -> %s", s, got)
		}
	}
}

func TestParseLSNErrors(t *testing.T) {
	for _, s := range []string{"", "abc", "0/", "/1", "G/1", "1/ZZ", "100000000/0"} {
		if _, err := ParseLSN(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}

func TestLag(t *testing.T) {
	p, _ := ParseLSN("0/3000060")
	r, _ := ParseLSN("0/3000000")
	if Lag(p, r) != 0x60 {
		t.Fatalf("lag = %d", Lag(p, r))
	}
	if Lag(r, p) != 0 {
		t.Fatal("lag must clamp at zero")
	}
	hi, _ := ParseLSN("1/0")
	lo, _ := ParseLSN("0/FFFFFFFF")
	if Lag(hi, lo) != 1 {
		t.Fatalf("lag across 4GiB boundary = %d", Lag(hi, lo))
	}
}

func TestParseControlRedo(t *testing.T) {
	out := "pg_control version number:            1300\nLatest checkpoint's REDO location:    0/3000028\nLatest checkpoint's REDO WAL file:    000000010000000000000003\n"
	l, err := ParseControlRedo(out)
	if err != nil || l.String() != "0/3000028" {
		t.Fatalf("got %v, %v", l, err)
	}
	if _, err := ParseControlRedo("nothing"); err == nil {
		t.Fatal("expected error")
	}
}
