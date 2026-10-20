<!-- Problem proposals come from your fork, on a problem/<id>-<slug> branch. -->

## Problem

- Id and title:
- Languages offered:

## Verification checklist

- [ ] `./drill lint` passes (at least 10 cases, all files present).
- [ ] `./drill test <id> --reference` passes in **every** offered
      language.
- [ ] The expected rows in `cases/` come from the problem's intent: worked
      out by hand or from an independent source, **not** copied from the
      reference solution's output.
- [ ] Cases cover edge cases: ties, empty input, NULLs, precision, ordering
      (when the output must be sorted).
- [ ] It isn't a duplicate of an existing problem (`drill lint` warns on
      similar titles).

## How the expected rows were derived

<!-- e.g. "computed by hand", "from the original HackerRank examples" -->
