-- Validate after the nullable column has been installed and its short DDL lock
-- has been released by the previous migration.
ALTER TABLE files VALIDATE CONSTRAINT files_subtype_rights_valid;
