package api

// Test-only aliases exposing unexported helpers to the external api_test package.
var (
	ValidatePhone = validatePhone
	VerifyToRPC   = verifyToRPC
	NewSentCode   = newSentCode
)
