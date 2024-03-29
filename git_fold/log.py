from __future__ import annotations

from typing import Any, Dict, List, Set

from .git import OID, call_git


class CommitGraph:
    @classmethod
    async def build_partial(cls, head: OID, roots: List[OID]) -> CommitGraph:
        """Build the commit graph from the head ref to the root refs

        This is implemented by call `git rev-list head ^root` for each root;
        I don't know of a way to handle it using a single git call.
        """
        graph = CommitGraph()

        for root in roots:
            # This root was discovered on the path to a prior root
            if root in graph:
                continue

            await graph.add_path(head, root)

        await graph.add_commits(roots)
        return graph

    def __init__(self) -> None:
        self.child_to_parents: Dict[OID, List[OID]] = {}

    def __contains__(self, commit_oid: Any) -> bool:
        return commit_oid in self.child_to_parents

    def get_parents(self, commit_oid: OID) -> List[OID]:
        return self.child_to_parents[commit_oid]

    async def add_commits(self, commits: List[OID]) -> None:
        """Add the specified commits to the graph"""
        _, output, _ = await call_git(
            'rev-list', '--parents', '--no-walk', *(str(c) for c in commits), '--'
        )
        self._add_from_rev_list_parents(output)

    async def add_path(self, head: OID, root: OID) -> None:
        """Add all commits on the ancestry path from head to root to the graph"""
        _, output, _ = await call_git(
            'rev-list', '--parents', '--ancestry-path', str(head), f'^{root}', '--'
        )
        self._add_from_rev_list_parents(output)

    def _add_from_rev_list_parents(self, output: bytes) -> None:
        for entry in output.split(b'\n'):
            if not entry:
                continue

            oids = entry.split()
            child = OID(oids[0])

            parents = [OID(p) for p in oids[1:]]

            try:
                prev = self.child_to_parents[child]
            except KeyError:
                self.child_to_parents[child] = parents
            else:
                assert parents == prev

    def reverse_topo_ordering(self, head: OID) -> List[OID]:
        """Return a reversed topological ordering starting at head

        This is a listing of the known ancestors of head such that each commit
        is listed before any of its descendants
        """
        visited: Set[OID] = set()
        ordering = []
        work_stack = [(head, self.child_to_parents[head], False)]

        while work_stack:
            child, parents, has_recursed = work_stack.pop()

            if not has_recursed:
                work_stack.append((child, parents, True))

                for parent in reversed(parents):
                    if parent not in visited:
                        try:
                            grandparents = self.child_to_parents[parent]
                        except KeyError:
                            continue

                        work_stack.append((parent, grandparents, False))
            else:
                visited.add(child)
                ordering.append(child)

        return ordering
