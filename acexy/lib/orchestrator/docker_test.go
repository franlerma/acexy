package orchestrator

import (
	"reflect"
	"testing"
)

func TestParseEngineArgs(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"only spaces", "   \t ", nil},
		{"single", "serve", []string{"serve"}},
		{"multiple with extra spaces", "  serve   --foo  bar ", []string{"serve", "--foo", "bar"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseEngineArgs(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseEngineArgs(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseEngineEnv(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []string
		wantErr bool
	}{
		{"empty", "", nil, false},
		{"single", "OUTPACE_ACE_COMPAT=1", []string{"OUTPACE_ACE_COMPAT=1"}, false},
		{"multiple with spaces", " A=1 , B=two ,, C= ", []string{"A=1", "B=two", "C="}, false},
		{"value containing equals", "URL=http://x?a=b", []string{"URL=http://x?a=b"}, false},
		{"missing equals", "OUTPACE_ACE_COMPAT", nil, true},
		{"empty key", "=1", nil, true},
		{"one invalid among valid", "A=1,broken,B=2", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseEngineEnv(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseEngineEnv(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseEngineEnv(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}

func TestEngineArgsDefaultsToAceStreamFlags(t *testing.T) {
	o := &Orchestrator{liveBuffer: 40}
	want := []string{
		"--client-console",
		"--bind-all",
		"--live-buffer", "40",
		"--live-cache-type", "memory",
	}
	if got := o.engineArgs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("engineArgs() = %#v, want %#v", got, want)
	}
}

func TestEngineArgsCustomReplacesDefaults(t *testing.T) {
	o := &Orchestrator{liveBuffer: 40, engineCmd: []string{"serve"}}
	want := []string{"serve"}
	if got := o.engineArgs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("engineArgs() = %#v, want %#v", got, want)
	}
}
