package pocketbase

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"github.com/spf13/cobra"
)

func TestNew(t *testing.T) {
	// copy os.Args
	originalArgs := make([]string, len(os.Args))
	copy(originalArgs, os.Args)
	defer func() {
		// restore os.Args
		os.Args = originalArgs
	}()

	// change os.Args
	os.Args = os.Args[:1]
	os.Args = append(
		os.Args,
		"--dir=test_dir",
		"--encryptionEnv=test_encryption_env",
		"--debug=true",
	)

	app := New()

	if app == nil {
		t.Fatal("Expected initialized PocketBase instance, got nil")
	}

	if app.RootCmd == nil {
		t.Fatal("Expected RootCmd to be initialized, got nil")
	}

	if app.App == nil {
		t.Fatal("Expected App to be initialized, got nil")
	}

	if app.DataDir() != "test_dir" {
		t.Fatalf("Expected app.DataDir() %q, got %q", "test_dir", app.DataDir())
	}

	if app.EncryptionEnv() != "test_encryption_env" {
		t.Fatalf("Expected app.EncryptionEnv() test_encryption_env, got %q", app.EncryptionEnv())
	}
}

func TestNewWithConfig(t *testing.T) {
	app := NewWithConfig(Config{
		DefaultDataDir:       "test_dir",
		DefaultEncryptionEnv: "test_encryption_env",
		HideStartBanner:      true,
	})

	if app == nil {
		t.Fatal("Expected initialized PocketBase instance, got nil")
	}

	if app.RootCmd == nil {
		t.Fatal("Expected RootCmd to be initialized, got nil")
	}

	if app.App == nil {
		t.Fatal("Expected App to be initialized, got nil")
	}

	if app.hideStartBanner != true {
		t.Fatal("Expected app.hideStartBanner to be true, got false")
	}

	if app.DataDir() != "test_dir" {
		t.Fatalf("Expected app.DataDir() %q, got %q", "test_dir", app.DataDir())
	}

	if app.EncryptionEnv() != "test_encryption_env" {
		t.Fatalf("Expected app.EncryptionEnv() %q, got %q", "test_encryption_env", app.EncryptionEnv())
	}
}

func TestNewWithConfigAndFlags(t *testing.T) {
	// copy os.Args
	originalArgs := make([]string, len(os.Args))
	copy(originalArgs, os.Args)
	defer func() {
		// restore os.Args
		os.Args = originalArgs
	}()

	// change os.Args
	os.Args = os.Args[:1]
	os.Args = append(
		os.Args,
		"--dir=test_dir_flag",
		"--encryptionEnv=test_encryption_env_flag",
		"--debug=false",
	)

	app := NewWithConfig(Config{
		DefaultDataDir:       "test_dir",
		DefaultEncryptionEnv: "test_encryption_env",
		HideStartBanner:      true,
	})

	if app == nil {
		t.Fatal("Expected initialized PocketBase instance, got nil")
	}

	if app.RootCmd == nil {
		t.Fatal("Expected RootCmd to be initialized, got nil")
	}

	if app.App == nil {
		t.Fatal("Expected App to be initialized, got nil")
	}

	if app.hideStartBanner != true {
		t.Fatal("Expected app.hideStartBanner to be true, got false")
	}

	if app.DataDir() != "test_dir_flag" {
		t.Fatalf("Expected app.DataDir() %q, got %q", "test_dir_flag", app.DataDir())
	}

	if app.EncryptionEnv() != "test_encryption_env_flag" {
		t.Fatalf("Expected app.EncryptionEnv() %q, got %q", "test_encryption_env_flag", app.EncryptionEnv())
	}
}

func TestSkipBootstrap(t *testing.T) {
	// copy os.Args
	originalArgs := make([]string, len(os.Args))
	copy(originalArgs, os.Args)
	defer func() {
		// restore os.Args
		os.Args = originalArgs
	}()

	tempDir := filepath.Join(os.TempDir(), "temp_pb_data")
	defer os.RemoveAll(tempDir)

	// already bootstrapped
	app0 := NewWithConfig(Config{DefaultDataDir: tempDir})
	app0.Bootstrap()
	if v := app0.skipBootstrap(); !v {
		t.Fatal("[bootstrapped] Expected true, got false")
	}

	// unknown command
	os.Args = os.Args[:1]
	os.Args = append(os.Args, "demo")
	app1 := NewWithConfig(Config{DefaultDataDir: tempDir})
	app1.RootCmd.AddCommand(&cobra.Command{Use: "test"})
	if v := app1.skipBootstrap(); !v {
		t.Fatal("[unknown] Expected true, got false")
	}

	// default flags
	flagScenarios := []struct {
		name  string
		short string
	}{
		{"help", "h"},
		{"version", "v"},
	}

	for _, s := range flagScenarios {
		// base flag
		os.Args = os.Args[:1]
		os.Args = append(os.Args, "--"+s.name)
		app1 := NewWithConfig(Config{DefaultDataDir: tempDir})
		if v := app1.skipBootstrap(); !v {
			t.Fatalf("[--%s] Expected true, got false", s.name)
		}

		// short flag
		os.Args = os.Args[:1]
		os.Args = append(os.Args, "-"+s.short)
		app2 := NewWithConfig(Config{DefaultDataDir: tempDir})
		if v := app2.skipBootstrap(); !v {
			t.Fatalf("[-%s] Expected true, got false", s.short)
		}

		customCmd := &cobra.Command{Use: "custom"}
		customCmd.PersistentFlags().BoolP(s.name, s.short, false, "")

		// base flag in custom command
		os.Args = os.Args[:1]
		os.Args = append(os.Args, "custom")
		os.Args = append(os.Args, "--"+s.name)
		app3 := NewWithConfig(Config{DefaultDataDir: tempDir})
		app3.RootCmd.AddCommand(customCmd)
		if v := app3.skipBootstrap(); v {
			t.Fatalf("[--%s custom] Expected false, got true", s.name)
		}

		// short flag in custom command
		os.Args = os.Args[:1]
		os.Args = append(os.Args, "custom")
		os.Args = append(os.Args, "-"+s.short)
		app4 := NewWithConfig(Config{DefaultDataDir: tempDir})
		app4.RootCmd.AddCommand(customCmd)
		if v := app4.skipBootstrap(); v {
			t.Fatalf("[-%s custom] Expected false, got true", s.short)
		}
	}
}

func withArgs(t *testing.T, args ...string) {
	originalArgs := os.Args
	t.Cleanup(func() {
		os.Args = originalArgs
	})

	os.Args = append([]string{originalArgs[0]}, args...)
}

func TestNewDBPoolsConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		withArgs(t)

		app := NewWithConfig(Config{})

		expected := []int{core.DefaultDataMaxOpenConns, core.DefaultDataMaxIdleConns, core.DefaultAuxMaxOpenConns, core.DefaultAuxMaxIdleConns}
		got := []int{app.dataMaxOpenConnsFlag, app.dataMaxIdleConnsFlag, app.auxMaxOpenConnsFlag, app.auxMaxIdleConnsFlag}
		if !slices.Equal(got, expected) {
			t.Fatalf("Expected pools config %v, got %v", expected, got)
		}
	})

	t.Run("env variables", func(t *testing.T) {
		withArgs(t)
		t.Setenv("PB_DATA_MAX_OPEN_CONNS", "11")
		t.Setenv("PB_DATA_MAX_IDLE_CONNS", "3")
		t.Setenv("PB_AUX_MAX_OPEN_CONNS", "invalid") // fallback to the default
		t.Setenv("PB_AUX_MAX_IDLE_CONNS", "1")

		app := NewWithConfig(Config{})

		expected := []int{11, 3, core.DefaultAuxMaxOpenConns, 1}
		got := []int{app.dataMaxOpenConnsFlag, app.dataMaxIdleConnsFlag, app.auxMaxOpenConnsFlag, app.auxMaxIdleConnsFlag}
		if !slices.Equal(got, expected) {
			t.Fatalf("Expected pools config %v, got %v", expected, got)
		}
	})

	t.Run("flags over env variables", func(t *testing.T) {
		withArgs(t, "--dataMaxOpenConns=12", "--dataMaxIdleConns=4", "--auxMaxOpenConns=5", "--auxMaxIdleConns=2")
		t.Setenv("PB_DATA_MAX_OPEN_CONNS", "11")
		t.Setenv("PB_AUX_MAX_OPEN_CONNS", "9")

		app := NewWithConfig(Config{})

		expected := []int{12, 4, 5, 2}
		got := []int{app.dataMaxOpenConnsFlag, app.dataMaxIdleConnsFlag, app.auxMaxOpenConnsFlag, app.auxMaxIdleConnsFlag}
		if !slices.Equal(got, expected) {
			t.Fatalf("Expected pools config %v, got %v", expected, got)
		}
	})
}

func TestPostgresURLCredentialsNotLeaked(t *testing.T) {
	secret := "pb" + "LeakCheck" + "42"

	t.Run("help output", func(t *testing.T) {
		withArgs(t)
		t.Setenv("POSTGRES_URL", "postgres://"+"leak_user:"+secret+"@127.0.0.1:1/postgres")

		app := NewWithConfig(Config{})

		if app.postgresUrlFlag != os.Getenv("POSTGRES_URL") {
			t.Fatalf("Expected the POSTGRES_URL env fallback, got %q", app.postgresUrlFlag)
		}

		// the flags part of the --help output
		help := app.RootCmd.UsageString()
		if !strings.Contains(help, "--postgresUrl") {
			t.Fatalf("Expected the --postgresUrl flag in the usage output:\n%s", help)
		}
		if strings.Contains(help, secret) {
			t.Fatalf("The --help output contains the connection string password:\n%s", help)
		}
	})

	t.Run("malformed url", func(t *testing.T) {
		withArgs(t)
		// "%zz" is an invalid url escape
		t.Setenv("POSTGRES_URL", "postgres://"+"leak_user:"+secret+"%zz@127.0.0.1:1/postgres")

		app := NewWithConfig(Config{DefaultDataDir: t.TempDir()})

		err := app.Bootstrap()
		if err == nil {
			t.Fatal("Expected Bootstrap error")
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("The error contains the connection string password: %v", err)
		}
	})
}
