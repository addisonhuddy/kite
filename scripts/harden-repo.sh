#!/usr/bin/env bash
# One-shot GitHub repo hardening (issue #45). Needs `gh` authenticated as a
# repo admin: gh auth login  (or GH_TOKEN with repo + administration scope).
#
#   scripts/harden-repo.sh [owner/repo]
set -euo pipefail

REPO=${1:-addisonhuddy/kite}
DESC='Ultra-lightweight Kafka CLI for agents and shell pipelines. One ~600 KB Zig binary, no JVM.'

echo "== repo settings: delete-branch-on-merge, no wiki/projects, description =="
gh api -X PATCH "repos/$REPO" \
    -F delete_branch_on_merge=true \
    -F has_wiki=false \
    -F has_projects=false \
    -f description="$DESC" >/dev/null

echo "== secret scanning + push protection =="
gh api -X PATCH "repos/$REPO" --input - >/dev/null <<'EOF'
{"security_and_analysis":{"secret_scanning":{"status":"enabled"},"secret_scanning_push_protection":{"status":"enabled"}}}
EOF

echo "== dependabot security updates =="
gh api -X PUT "repos/$REPO/automated-security-fixes" >/dev/null

echo "== topics =="
gh api -X PUT "repos/$REPO/topics" \
    -f 'names[]=kafka' -f 'names[]=zig' -f 'names[]=cli' -f 'names[]=agents' >/dev/null

echo "== branch protection on main =="
gh api -X PUT "repos/$REPO/branches/main/protection" --input - >/dev/null <<'EOF'
{
  "required_status_checks": {"strict": true, "contexts": ["test", "e2e"]},
  "enforce_admins": false,
  "required_pull_request_reviews": {"required_approving_review_count": 0},
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "required_conversation_resolution": true
}
EOF

echo "== result =="
gh api "repos/$REPO" -q '{delete_branch_on_merge, has_wiki, has_projects, description, topics, security_and_analysis}'
gh api "repos/$REPO/branches/main/protection" -q '{required_status_checks: .required_status_checks.contexts, allow_force_pushes: .allow_force_pushes.enabled, allow_deletions: .allow_deletions.enabled}'
echo "ok"
