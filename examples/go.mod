module gitlab.com/taskiq/taskiq/examples

go 1.18

require (
	github.com/go-redis/redis/v8 v8.11.5
	github.com/google/uuid v1.3.0
	gitlab.com/taskiq/taskiq/pkg/taskiq v0.0.0-unpublished
)

// Replace directive to use the local taskiq library
replace gitlab.com/taskiq/taskiq/pkg/taskiq => ../pkg/taskiq

// Transitive dependencies from go-redis, if any, will be listed here by 'go mod tidy'
// For example:
// require (
// 	github.com/cespare/xxhash/v2 v2.1.2 // indirect
// 	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
// )
// We'll run 'go mod tidy' later to populate these.
