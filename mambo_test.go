package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ports    string
		freq     string
		wantPort int
	}{
		{"defaults", "", "1000", 8125},
		{"legacy port", "stats_port = 9000", "1000", 9000},
		{"documented port takes precedence", "stats_port = 9000\nstatsd_port = 9001", "1000", 9001},
		{"missing frequency", "", "", 0},
		{"invalid frequency", "", "bad", 0},
		{"zero frequency", "", "0", 0},
		{"negative frequency", "", "-1", 0},
		{"overflowing duration", "", "9223372036855", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "mambo.cfg")
			config := fmt.Sprintf("[config]\n%s\n[query1]\nkey = mysql.test\nquery = SELECT 1\n", tc.ports)
			if tc.freq != "" {
				config += "freq = " + tc.freq + "\n"
			}
			if err := os.WriteFile(path, []byte(config), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, commands, err := configure(path)
			if tc.wantPort == 0 {
				if err == nil {
					t.Fatal("configure accepted an invalid frequency")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.statsdPort != tc.wantPort || cfg.mysqlPort != 3306 {
				t.Fatalf("got StatsD port %d and MySQL port %d", cfg.statsdPort, cfg.mysqlPort)
			}
			if len(commands) != 1 || commands[0].interval != time.Second {
				t.Fatalf("got commands %+v, want one command with a 1s interval", commands)
			}
		})
	}
}

func TestMySQLConfig(t *testing.T) {
	socket := mysqlConfig(&configuration{})
	if socket.Net != "unix" {
		t.Fatalf("got network %q, want unix", socket.Net)
	}
	tcp := mysqlConfig(&configuration{mysqlHost: "::1", mysqlPort: 3307})
	if tcp.Net != "tcp" || tcp.Addr != "[::1]:3307" {
		t.Fatalf("got network %q and address %q, want tcp and [::1]:3307", tcp.Net, tcp.Addr)
	}
}

func TestStatsdSenderHandlesInvalidAddress(t *testing.T) {
	statsdSender(&configuration{statsdHost: "127.0.0.1", statsdPort: -1}, metric{key: "mysql.test", value: 1})
}
