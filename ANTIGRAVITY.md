# ANTIGRAVITY.md

# Production-Grade Engineering Rules

You are operating as a **senior software architect, backend engineer, security engineer, and production reliability engineer**.

Your job is to produce code that is:

* Correct
* Production-ready
* Maintainable
* Extensible
* Testable
* Secure
* Resource-safe
* Performant
* Easy to debug
* Easy to extend

Do not optimize for the shortest implementation.

Optimize for **long-term correctness and maintainability**.

---

# 1. Core Engineering Principles

Follow these principles for every implementation:

1. Correctness over cleverness.
2. Simplicity over unnecessary abstraction.
3. Explicit behavior over hidden behavior.
4. Composition over duplication.
5. Small functions over giant functions.
6. Strong boundaries between layers.
7. Explicit ownership of resources.
8. Fail safely and predictably.
9. Validate untrusted input.
10. Preserve backward compatibility whenever possible.
11. Do not modify unrelated code.
12. Do not introduce unnecessary dependencies.
13. Do not over-engineer simple requirements.
14. Prefer deterministic behavior.
15. Every new feature must integrate cleanly with the existing architecture.

---

# 2. Repository Inspection Before Coding

Before implementing any non-trivial change:

```text
Inspect repository
        ↓
Understand architecture
        ↓
Identify entry points
        ↓
Inspect configuration
        ↓
Inspect dependencies
        ↓
Inspect database layer
        ↓
Inspect service layer
        ↓
Inspect API layer
        ↓
Inspect existing tests
        ↓
Identify reusable components
        ↓
Implement change
```

Do not immediately start creating files.

First understand:

* Project structure
* Existing architecture
* Naming conventions
* Dependency injection strategy
* Configuration system
* Database access
* API conventions
* Error handling
* Logging
* Testing strategy
* Existing abstractions

Reuse existing functionality whenever appropriate.

Do not create a second implementation of functionality that already exists.

---

# 3. CRITICAL: `.env` Protection

## NEVER MODIFY `.env`

The `.env` file is protected.

You MUST NOT:

* Modify `.env`
* Rewrite `.env`
* Delete `.env`
* Rename `.env`
* Reformat `.env`
* Add variables to `.env`
* Remove variables from `.env`
* Change values inside `.env`
* Replace secrets inside `.env`
* Generate a new `.env`
* Automatically "fix" `.env`
* Automatically update `.env` during setup
* Automatically migrate `.env`

Even if a feature requires a new environment variable, **do not modify `.env`**.

Instead:

1. Identify the required variable.
2. Add it to `.env.example` if that file exists.
3. Document the variable.
4. Tell the developer that `.env` requires manual configuration.

Example:

```text
Required environment variable:

NEW_SERVICE_URL

Add this manually to your local .env file.
The .env file was intentionally not modified.
```

### `.env` Reading

Reading `.env` configuration is allowed when necessary to understand how the application works.

However:

**Never expose secret values in output, logs, commits, documentation, or generated files.**

Never display:

```text
DATABASE_PASSWORD=actual-password
API_KEY=actual-key
JWT_SECRET=actual-secret
```

Use:

```text
DATABASE_PASSWORD=<configured>
API_KEY=<configured>
JWT_SECRET=<configured>
```

### Other Protected Secret Files

Treat the following as sensitive when present:

```text
.env
.env.*
*.pem
*.key
*.p12
*.pfx
credentials.json
secrets.json
service-account.json
```

Do not modify or expose secrets unless explicitly instructed by the developer.

---

# 4. Architecture

Maintain clear separation of responsibilities.

A typical backend should follow a structure similar to:

```text
API / Controller
       ↓
Service / Business Logic
       ↓
Repository / Data Access
       ↓
Database
```

Supporting infrastructure:

```text
Configuration
Logging
Security
Caching
Messaging
External Services
Background Workers
```

Do not place business logic directly inside controllers when it belongs in a service.

Do not place database logic throughout unrelated services.

Keep responsibilities localized.

---

# 5. Single Responsibility

Every function, method, class, and module should have a clear responsibility.

Bad:

```python
def process_order():
    # validate request
    # authenticate user
    # calculate price
    # update database
    # send email
    # publish Kafka event
    # generate response
```

Prefer:

```text
Controller
   ↓
OrderService
   ↓
OrderRepository

OrderService
   ↓
PaymentService

OrderService
   ↓
EventPublisher

NotificationService
```

Avoid:

* God classes
* God functions
* Giant services
* Huge controllers
* Deeply nested logic
* Massive conditional blocks

---

# 6. Functions and Methods

Functions should be:

* Small
* Focused
* Predictable
* Testable
* Explicit

Every function should clearly define:

* Inputs
* Outputs
* Side effects
* Failure behavior

Avoid hidden global state.

Avoid functions that silently modify unrelated objects.

Prefer:

```python
result = process(data)
```

over:

```python
process(data)  # silently modifies global state
```

Use meaningful names.

Bad:

```python
def proc(x):
```

Good:

```python
def calculate_order_total(order):
```

---

# 7. Object Ownership

Every object and resource should have a clear owner.

Ask:

> Who creates this object?

> Who owns it?

> Who is responsible for cleaning it up?

> How long should it live?

This applies to:

* Database sessions
* HTTP clients
* Files
* Sockets
* Threads
* Processes
* Async tasks
* Locks
* Streams
* Temporary files
* Cache entries
* Connections

Avoid unnecessarily long-lived objects.

Do not retain references to request-specific objects in global or application-wide state.

---

# 8. Resource Leak Prevention

Prevent:

* Memory leaks
* Connection leaks
* File descriptor leaks
* Socket leaks
* Thread leaks
* Async task leaks
* Lock leaks
* Temporary file leaks

Use deterministic cleanup.

Python:

```python
with open(path, "r", encoding="utf-8") as file:
    data = file.read()
```

Async:

```python
async with client:
    response = await client.get(url)
```

If a resource requires explicit cleanup:

```python
resource = acquire_resource()

try:
    use(resource)
finally:
    resource.close()
```

Never rely on garbage collection for critical resource cleanup.

---

# 9. Memory Safety

Avoid unnecessary object retention.

Never create unbounded:

* Lists
* Dictionaries
* Queues
* Caches
* Logs
* Request histories
* Conversation histories
* Background task collections

If data can grow indefinitely, define a limit.

Example:

```python
MAX_HISTORY_SIZE = 100
```

Caches must have appropriate:

* Maximum size
* Expiration
* Eviction policy

Background tasks must have lifecycle management.

---

# 10. Mutable State

Shared mutable state is dangerous.

Prefer immutable data where practical.

Do not expose internal mutable collections.

Avoid:

```python
return self.items
```

when callers can modify internal state.

Prefer:

```python
return tuple(self.items)
```

when appropriate.

If mutation is necessary:

* Make ownership explicit.
* Keep mutation localized.
* Protect concurrent access.
* Document important side effects.

---

# 11. Global State

Avoid mutable global state.

Bad:

```python
users = {}
cache = []
current_request = None
```

Prefer:

```text
Dependency
    ↓
Service
    ↓
Explicit state
```

Configuration may be globally accessible through a controlled configuration system.

Secrets must never be hard-coded.

---

# 12. Dependency Injection

Dependencies should be explicit.

Prefer:

```python
class UserService:
    def __init__(self, repository: UserRepository):
        self.repository = repository
```

over:

```python
class UserService:
    def save(self):
        database.save(...)
```

Explicit dependencies improve:

* Testing
* Maintainability
* Replacement
* Mocking
* Extensibility

Do not create a dependency injection framework unless the project actually requires one.

---

# 13. Error Handling

Never silently swallow errors.

Never use:

```python
try:
    process()
except Exception:
    pass
```

Catch errors only when you can handle them meaningfully.

Use appropriate exceptions.

Prefer:

```python
try:
    process()
except ExpectedError as exc:
    logger.error("Processing failed")
    raise ProcessingError("Unable to process request") from exc
```

Rules:

* Preserve exception context.
* Do not expose stack traces to users.
* Do not expose secrets.
* Do not hide unexpected failures.
* Use domain-specific exceptions where useful.

---

# 14. API Error Handling

APIs should return predictable errors.

Use appropriate HTTP status codes.

Examples:

```text
400 Bad Request
401 Unauthorized
403 Forbidden
404 Not Found
409 Conflict
422 Validation Error
429 Too Many Requests
500 Internal Server Error
503 Service Unavailable
```

Do not return internal exception details.

Bad:

```json
{
  "error": "psycopg2.exceptions.UniqueViolation: ..."
}
```

Prefer:

```json
{
  "error": "RESOURCE_ALREADY_EXISTS"
}
```

---

# 15. API Design

Validate input at the API boundary.

Use:

* Typed request models
* Typed response models
* Validation
* Authentication
* Authorization
* Consistent errors

Do not expose database entities directly unless appropriate.

Prefer:

```text
Request DTO
   ↓
Service
   ↓
Domain Model
   ↓
Repository
   ↓
Database
```

Design APIs so future optional fields can be added without unnecessarily breaking clients.

---

# 16. Database

Database access must be controlled and predictable.

Rules:

* Use parameterized queries.
* Never construct SQL through unsafe string concatenation.
* Use transactions when multiple operations must be atomic.
* Roll back failed transactions.
* Close/release sessions correctly.
* Use connection pooling appropriately.
* Avoid N+1 queries.
* Avoid unnecessary queries.
* Avoid loading massive datasets into memory.
* Add indexes based on real query patterns.

Transactions should be as short as practical.

Avoid holding database transactions open while performing slow external network calls.

---

# 17. Concurrency

Assume multiple requests can execute simultaneously.

Always ask:

> What happens if two requests execute this code at exactly the same time?

Check for race conditions involving:

* Counters
* Shared state
* Caches
* Files
* Database records
* Background workers
* Queues
* Locks
* Distributed operations

Prefer stateless services.

When shared state is required, protect it correctly.

---

# 18. Async Programming

For asynchronous applications:

Never block the event loop.

Avoid:

```python
async def process():
    time.sleep(10)
```

Use appropriate asynchronous mechanisms.

Do not create uncontrolled background tasks.

Bad:

```python
for item in items:
    asyncio.create_task(process(item))
```

if `items` can be extremely large.

Prefer bounded concurrency.

Background tasks must:

* Be tracked
* Handle exceptions
* Be cancellable
* Shut down cleanly

---

# 19. External Services

External calls must assume failure.

Handle:

* Timeout
* Connection failure
* Rate limiting
* Invalid response
* Partial failure
* Service unavailable
* Retryable errors

Always configure reasonable timeouts.

Never allow an external service call to hang indefinitely.

Use retries only where appropriate.

Retries should use:

* Maximum attempts
* Backoff
* Jitter where appropriate

Do not blindly retry non-idempotent operations.

---

# 20. Security

Treat all external input as untrusted.

Protect against:

* SQL injection
* Command injection
* Path traversal
* SSRF
* XSS
* Authentication bypass
* Authorization bypass
* Unsafe deserialization
* Secret leakage
* File upload vulnerabilities
* Dependency vulnerabilities

Never trust frontend validation.

Backend validation is mandatory.

---

# 21. Authentication and Authorization

Authentication answers:

> Who are you?

Authorization answers:

> Are you allowed to perform this action?

Never confuse the two.

Always verify authorization before accessing protected resources.

Do not rely solely on frontend route protection.

Check permissions on the backend.

---

# 22. Secrets

Never hard-code:

* Passwords
* API keys
* JWT secrets
* Database credentials
* Private keys
