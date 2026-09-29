# Security Policy

## No secrets in this repository

AXOS has no secret inputs of any kind — no API keys, tokens,
passwords, private keys, cloud credentials, database credentials,
cookies, session tokens, or service-account credentials. This is a
design property, verified by inspection of the frozen source:

- AXOS reads **zero** environment variables for configuration (see
  `config/ENVIRONMENT_CONTRACT.md`).
- Filename search over the tree (`*secret*`, `*.env*`, `*credential*`,
  `*.pem`, `*.key`) returns no hits.
- The ledger's tamper-evidence comes from SHA-256 hash chaining, not
  from cryptographic keys — there are no keys to leak.

If you find anything in this repository that looks like a credential,
treat it as a security issue and report it as below.

## Reporting a vulnerability

**Do not open a public issue for a security problem.** Use GitHub's
private vulnerability reporting for this repository
(Security tab → "Report a vulnerability"), so the details stay private
until a fix is available.

Include:

- A description of the issue and its potential impact.
- Steps to reproduce (commands, file paths, line numbers).
- The release identity (`src/axos/release_manifest.json`
  `release_id`) you tested against.

Expect an acknowledgment and a triage decision. There is no bug-bounty
program.

## Secret-scan procedure (before every push)

Maintainers run this before any push to `main`:

```bash
# Pattern scan over tracked content
git grep -n -i -E "AKIA|BEGIN PRIVATE KEY|PRIVATE KEY|password=|passwd=|token=|secret=|api_key=|Authorization:|Bearer " -- . \
  | grep -v -E "docs/|SECURITY.md|CHANGELOG.md" || true

# Dangerous filenames anywhere in the tree or history
git ls-files | grep -i -E "\.env$|\.pem$|\.key$|credentials|secrets" || true
```

Notes:

- Documentation legitimately discusses the *absence* of secrets and
  shows scan patterns; hits inside `docs/` and `SECURITY.md` that are
  clearly descriptive are not findings.
- `.gitignore` is not sufficient on its own: inspect the working tree
  **and** the git history before the first push. A secret found in
  history must be removed from history (e.g. via `git filter-repo`),
  not merely deleted from the working tree.
- Never print private credentials in reports, issues, or logs.

## Operational security notes

- **Ledger is tamper-evident, not tamper-proof.** A writer with raw
  database access can rewrite the hash chain. Protect the state
  database (`<state-root>/state/axos.db`) at the filesystem level:
  restrict file permissions, run AXOS as a dedicated user, and keep
  backups offline.
- **No network surface.** AXOS opens no ports, makes no outbound
  connections, and has no remote API — there is nothing to firewall
  beyond the host itself.
- **Process signaling.** Fencing uses `SIGTERM`/`SIGKILL` via
  `os.killpg` on process groups AXOS created. Do not run AXOS in a
  security profile that blocks signaling its own process groups, and do
  not grant it broader signaling privileges than it needs.
- **Worker debug logs** (`/tmp/axos-<proc_id>.log`) are
  non-authoritative and may contain workload payloads. Treat them with
  the same care as any application log.

## Supported versions

| Version | Status |
|---|---|
| v0.1.0 (`551d559c…`, migration v12) | Supported — this is the only release |

Security fixes, if ever needed, would be issued as a new release with a
new release identity; the frozen v0.1.0 tree itself is never rewritten.
