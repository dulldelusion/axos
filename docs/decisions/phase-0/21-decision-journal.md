# 21 — Decision Journal

Consequential decisions are recorded with their reasoning, because "the
machine did X" is not an explanation. The journal is what makes autonomy
auditable — and what lets a future incident review distinguish a good
decision with a bad outcome from a bad decision.

## 21.1 Entry format

```
decision_id, task_id?, at (store time),
question,            — the decision being made
evidence[],         — ledger refs, metrics, observations
alternatives[],     — options considered, with expected outcomes
selected,           — what was chosen
reason,             — why, in terms of the evidence
confidence,         — high | medium | low
actor,              — component, worker, or human
reversible?         — and the reversal cost
```

## 21.2 What gets journaled (consequential only)

- Why a worker was replaced vs restarted (diagnosis + confidence).
- Why a strategy changed (rung 8) or a replan was issued (rung 9).
- Why a source/input was rejected or quarantined.
- Why content was redesigned or a prompt/methodology version changed.
- Why execution was paused or a safe stop triggered.
- Why a circuit breaker was manually closed.
- Why a methodology was selected or invalidated.
- Why a human was asked (what the machine couldn't decide).

Routine operations (a single retry, a lease renewal, a scheduled checkpoint)
are ledger events, not journal entries. The journal is for *judgment*.

## 21.3 Rules

1. **Low confidence → escalate, don't journal-and-proceed.** A low-
   confidence consequential decision is itself a reason to involve a higher
   rung or a human.
2. **Decisions link to evidence, not vibes.** Every entry references ledger
   events, metric snapshots, or artifact hashes. "Felt wrong" is not
   evidence.
3. **The journal is append-only.** A reversed decision gets a new entry
   referencing the old one. History is not rewritten.
4. **Provenance links here.** Artifacts affected by a decision carry its id
   (`19`), so "why does this output look like that?" terminates at a
   reasoned decision, not a shrug.
