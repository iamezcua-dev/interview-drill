from gitlint.rules import CommitRule, RuleViolation


class BulletFormatRule(CommitRule):
    """Enforce body bullet formatting.

    Rules per bullet:
      - Starts with "- " and the next character must be uppercase.
      - May span multiple lines if continuation lines start with 2 spaces.
      - Final line must end with '.'.
    """

    name = "body-bullet-format"
    id = "UC1"

    def validate(self, commit):
        violations = []
        body = commit.message.body or []

        bullet_lines = []
        bullet_start_lineno = None

        def flush(lines, start_lineno):
            if not lines:
                return
            first = lines[0]
            after_dash = first[2:3]
            if not after_dash.isupper():
                violations.append(
                    RuleViolation(
                        self.id,
                        "Bullet must start with an uppercase letter",
                        first,
                        start_lineno,
                    )
                )
            last = lines[-1].rstrip()
            if not last.endswith("."):
                violations.append(
                    RuleViolation(
                        self.id,
                        "Bullet must end with a period",
                        last,
                        start_lineno,
                    )
                )

        for idx, line in enumerate(body):
            line_no = idx + 2  # body line numbers start after title + blank line

            if line.startswith("- "):
                flush(bullet_lines, bullet_start_lineno)
                bullet_lines = [line]
                bullet_start_lineno = line_no
            elif line.startswith("  ") and bullet_lines:
                bullet_lines.append(line)
            elif line.strip() == "":
                flush(bullet_lines, bullet_start_lineno)
                bullet_lines = []
                bullet_start_lineno = None
            else:
                flush(bullet_lines, bullet_start_lineno)
                bullet_lines = []
                bullet_start_lineno = None

        flush(bullet_lines, bullet_start_lineno)

        return violations
