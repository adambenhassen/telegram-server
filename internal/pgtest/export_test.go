package pgtest

// Test-only handles on the readiness internals, so the external test package can
// exercise them without the container-setup path around them.
var (
	ContainerOptions = containerOptions
	WaitAccepting    = waitAccepting
)
