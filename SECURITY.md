# Publication and Privacy

## Before Publishing

- Review tracked files, staged changes, and Git history, not just the current
  source files. Deleted files and previous versions remain in history.
- Keep credentials, private keys, local configuration, benchmark results, and
  packet captures out of commits. The ignore rules cover common filenames, not
  every possible secret or custom output directory. Never force-add these files.
- `.env.example` and `.env.sample` are intentionally allowed; use placeholders
  only. Use reserved documentation IP addresses (`192.0.2.0/24`,
  `198.51.100.0/24`, `203.0.113.0/24`, and `2001:db8::/32`) in fixtures and
  examples rather than real infrastructure addresses.
- Inspect commit author and committer identities. Configure a GitHub-provided
  noreply address before creating public commits if your email must stay
  private. This does not change existing commits.
- Review AI-attribution notes under `refs/notes/*` separately. Depending on the
  tooling, notes can contain author identities, session metadata, or prompts.
  Ordinary branch pushes do not include notes, but explicit notes pushes,
  tooling integrations, or mirror pushes can publish them.
- `.gitignore` does not untrack committed files, redact Git history, or protect
  manually uploaded ZIP archives and release attachments.

Useful local checks:

```bash
git status --short --ignored
git diff --cached --stat
git diff --cached
git log --all --format=fuller
git for-each-ref --format='%(refname)'
```

Review these outputs locally; they may themselves contain private information.
Run a dedicated secret scanner against the working tree and full history before
a public release. Automated scans cannot prove that all private information is
absent.

If a credential was committed, revoke or rotate it first. Deleting it from the
latest version is insufficient. Coordinate any history cleanup with repository
owners before rewriting commits or force-pushing.

## Runtime Data

Benchmark JSON and text logs can contain endpoint addresses, source ports, host
telemetry, hostnames, and local filesystem paths from errors. The default `logs/`
contents are ignored, but custom log directories require separate review.

`fullbench` defaults `--vm-name` to the local hostname. Use a non-sensitive label
when sharing results. HTTP export is opt-in via `--export-url`; review the full
payload and use a trusted endpoint. `--export-slim` reduces detail but is not
anonymization. Export URLs are printed and saved in manifests, and HTTP error
response bodies may appear in logs. Do not put credentials in export URLs.

Authentication headers supplied on the command line may be visible in process
listings and shell history. Never paste real header values into public issues,
documentation, or scripts.

MTR annotation performs reverse-DNS lookups and, for public addresses, RDAP/ASN
lookups through `ipwhois`. These lookups disclose the queried addresses to the
relevant DNS and lookup services.
