package main

import "testing"

func TestAddressForAcceptsLoopbackAndContainerBindAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, host, want string
	}{
		{name: "loopback", host: "127.0.0.1", want: "127.0.0.1:8082"},
		{name: "container", host: "0.0.0.0", want: "0.0.0.0:8082"},
		{name: "IPv6 loopback", host: "::1", want: "[::1]:8082"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := addressFor(tc.host, 8082)
			if err != nil {
				t.Fatalf("addressFor(%q): %v", tc.host, err)
			}
			if got != tc.want {
				t.Errorf("addressFor(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}

func TestAddressForRejectsInvalidListenAddresses(t *testing.T) {
	t.Parallel()

	for _, host := range []string{"", "not-an-ip", "127.0.0.1:8082", " localhost "} {
		if _, err := addressFor(host, 8082); err == nil {
			t.Errorf("addressFor(%q) succeeded, want an error", host)
		}
	}
}
