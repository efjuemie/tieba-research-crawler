from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from src.research.models import ResearchComment
from src.research.web_fallback.tasks import _membership_predicate


def test_postgresql_web_enrichment_lists_use_one_array_bind_each():
    tids = list(range(4_889))
    parent_pids = list(range(27_882))
    cids = [1]
    statement = select(ResearchComment.cid).where(
        _membership_predicate(ResearchComment.tid, tids, use_postgres_array=True),
        _membership_predicate(ResearchComment.parent_post_pid, parent_pids, use_postgres_array=True),
        _membership_predicate(ResearchComment.cid, cids, use_postgres_array=True),
    )

    compiled = statement.compile(dialect=postgresql.dialect())

    assert len(compiled.params) == 3
    assert str(compiled).count("ANY (") == 3
    assert sorted(len(value) for value in compiled.params.values()) == [1, 4_889, 27_882]
