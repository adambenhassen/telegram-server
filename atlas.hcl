# Atlas project config. See docs/migrations.md.
#
# url  — target database migrations are applied to (set TG_POSTGRES_DSN).
# dev  — a throwaway "dev database" Atlas spins up to parse/execute the
#        migration SQL for validate/diff. docker://postgres/16 matches the
#        Postgres 16 image used in tests and production.
env "local" {
  url = getenv("TG_POSTGRES_DSN")
  dev = "docker://postgres/16/dev?search_path=public"

  migration {
    dir = "file://migrations"
  }
}
