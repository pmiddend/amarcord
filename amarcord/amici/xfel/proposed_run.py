from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class ProposedRun:
    run_id: int
    proposal_id: int

    def __repr__(self) -> str:
        return f"(proposal {self.proposal_id}, run {self.run_id})"

    def __str__(self) -> str:
        return f"(proposal {self.proposal_id}, run {self.run_id})"
