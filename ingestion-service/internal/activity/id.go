package activity

import (
	"crypto/rand"
	"fmt"
)

// newID generates a random UUID v4 string (e.g.
// "f47ac10b-58cc-4372-a567-0e02b2c3d479"). A small hand-rolled generator
// rather than a dependency like google/uuid: event/session IDs here only
// need to be unique-looking correlation strings for simulated traffic,
// not cryptographically significant identifiers, so pulling in a whole
// package for ~10 lines of RFC 4122 bit-twiddling isn't worth it.
func newID() string {
	var b [16]byte
	// crypto/rand.Read on a fixed-size buffer only fails if the OS's
	// entropy source is unavailable, which would mean the process is
	// broken in ways far beyond this function's ability to recover from -
	// panicking here (rather than plumbing an error return through every
	// event-generation call site) is deliberate.
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("activity: reading random bytes for id: %v", err))
	}
	// Set version (4) and variant (RFC 4122) bits per the UUID v4 spec.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
