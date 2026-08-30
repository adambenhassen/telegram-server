package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/config"
)

var blobS3EnvNames = []string{
	"TG_BLOB_S3_ENDPOINT",
	"TG_BLOB_S3_BUCKET",
	"TG_BLOB_S3_PREFIX",
	"TG_BLOB_S3_REGION",
	"TG_BLOB_S3_ACCESS_KEY_ID",
	"TG_BLOB_S3_SECRET_ACCESS_KEY",
	"TG_BLOB_S3_SECRET_ACCESS_KEY_FILE",
	"TG_BLOB_S3_CA_PATH",
	"TG_BLOB_S3_ALLOW_INSECURE_HTTP",
}

func clearBlobS3Env(t *testing.T) {
	t.Helper()
	for _, name := range blobS3EnvNames {
		value, ok := os.LookupEnv(name)
		if !ok {
			value = ""
		}
		t.Setenv(name, value)
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
}

func setBlobS3Required(t *testing.T) {
	t.Helper()
	t.Setenv("TG_BLOB_S3_ENDPOINT", "https://objects.example.test")
	t.Setenv("TG_BLOB_S3_BUCKET", "telegram")
	t.Setenv("TG_BLOB_S3_PREFIX", "server-a")
	t.Setenv("TG_BLOB_S3_ACCESS_KEY_ID", "access")
	t.Setenv("TG_BLOB_S3_SECRET_ACCESS_KEY", "secret")
}

func TestLoadBlobS3UnsetUsesLocalDefault(t *testing.T) {
	clearBlobS3Env(t)
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BlobS3 != nil {
		t.Fatalf("BlobS3 = %+v, want nil when object-store configuration is absent", cfg.BlobS3)
	}
}

func TestLoadBlobS3EmptySettingsKeepLocalDefault(t *testing.T) {
	for _, name := range blobS3EnvNames {
		t.Run(name, func(t *testing.T) {
			clearBlobS3Env(t)
			t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
			t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
			t.Setenv(name, "")

			cfg, err := config.Load(discardLog())
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.BlobS3 != nil {
				t.Fatalf("BlobS3 = %+v, want nil for empty %s", cfg.BlobS3, name)
			}
		})
	}
}

func TestLoadBlobS3FromSecretFile(t *testing.T) {
	clearBlobS3Env(t)
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	setBlobS3Required(t)
	secretPath := filepath.Join(t.TempDir(), "s3-secret")
	if err := os.WriteFile(secretPath, []byte("file-secret\n"), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	t.Setenv("TG_BLOB_S3_SECRET_ACCESS_KEY", "")
	t.Setenv("TG_BLOB_S3_SECRET_ACCESS_KEY_FILE", secretPath)
	t.Setenv("TG_BLOB_S3_REGION", "eu-west-1")
	t.Setenv("TG_BLOB_S3_ALLOW_INSECURE_HTTP", "true")

	cfg, err := config.Load(discardLog())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BlobS3 == nil {
		t.Fatal("BlobS3 is nil")
	}
	if cfg.BlobS3.Endpoint != "https://objects.example.test" || cfg.BlobS3.Bucket != "telegram" || cfg.BlobS3.Prefix != "server-a" ||
		cfg.BlobS3.Region != "eu-west-1" || cfg.BlobS3.AccessKeyID != "access" || cfg.BlobS3.SecretAccessKey != "file-secret" || !cfg.BlobS3.AllowInsecureHTTP {
		t.Fatalf("BlobS3 = %+v, want configured file-backed credentials", cfg.BlobS3)
	}
}

func TestLoadBlobS3RequiresEverySetting(t *testing.T) {
	for _, missing := range []string{
		"TG_BLOB_S3_ENDPOINT",
		"TG_BLOB_S3_BUCKET",
		"TG_BLOB_S3_PREFIX",
		"TG_BLOB_S3_ACCESS_KEY_ID",
		"TG_BLOB_S3_SECRET_ACCESS_KEY",
	} {
		t.Run(missing, func(t *testing.T) {
			clearBlobS3Env(t)
			t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
			t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
			setBlobS3Required(t)
			if missing == "TG_BLOB_S3_SECRET_ACCESS_KEY" {
				t.Setenv("TG_BLOB_S3_SECRET_ACCESS_KEY", "")
			} else {
				t.Setenv(missing, "")
			}
			_, err := config.Load(discardLog())
			if err == nil || !strings.Contains(err.Error(), missing) {
				t.Fatalf("Load error = %v, want an error naming %s", err, missing)
			}
		})
	}
}

func TestLoadBlobS3RejectsAmbiguousSecretSources(t *testing.T) {
	clearBlobS3Env(t)
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	setBlobS3Required(t)
	secretPath := filepath.Join(t.TempDir(), "s3-secret")
	if err := os.WriteFile(secretPath, []byte("file-secret"), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	t.Setenv("TG_BLOB_S3_SECRET_ACCESS_KEY_FILE", secretPath)
	_, err := config.Load(discardLog())
	if err == nil || !strings.Contains(err.Error(), "TG_BLOB_S3_SECRET_ACCESS_KEY") || !strings.Contains(err.Error(), "TG_BLOB_S3_SECRET_ACCESS_KEY_FILE") {
		t.Fatalf("Load error = %v, want both secret variable names", err)
	}
}

func TestLoadBlobS3SecretErrorsNeverExposeSecret(t *testing.T) {
	clearBlobS3Env(t)
	t.Setenv("TG_POSTGRES_DSN", "postgres://localhost/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", validEncKey)
	const secret = "do-not-leak-this-value"
	t.Setenv("TG_BLOB_S3_SECRET_ACCESS_KEY", secret)
	_, err := config.Load(discardLog())
	if err == nil {
		t.Fatal("Load succeeded with incomplete object-store configuration")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("Load error %q exposes the configured secret", err)
	}
}
