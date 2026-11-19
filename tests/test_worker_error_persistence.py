from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import make_comment, make_comments_response

from src.core.lock import MemoryLockManager
from src.scraper.router import QueueRouter
from src.scraper.tasks import FullScanCommentsTask
from src.scraper.worker import FullScanCommentsTaskHandler


class _CodeError(Exception):
    def __init__(self, code: object):
        self.code = code


def _comments_handler(datastore):
    container = SimpleNamespace(lock_manager=MemoryLockManager())
    return FullScanCommentsTaskHandler(0, container, datastore, QueueRouter())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "status", "scope_status", "stored_code", "reason"),
    [
        ("rvf5", "failed", "incomplete", None, "_CodeError"),
        ([], "failed", "incomplete", None, "_CodeError"),
        (4011, "paused_auth", "paused_auth", 4011, "_CodeError"),
        (429, "failed", "incomplete", 429, "_CodeError"),
        (220034, "failed", "incomplete", 220034, "_CodeError"),
        (4, "blocked", "blocked", 4, "content_unavailable"),
    ],
)
async def test_detail_failure_persists_only_normalized_integer_codes(
    code, status, scope_status, stored_code, reason
):
    datastore = SimpleNamespace(
        record_research_checkpoint=AsyncMock(return_value=True),
        set_research_run_status_if=AsyncMock(),
    )
    handler = _comments_handler(datastore)

    await handler._record_detail_failure(
        run_id="synthetic-error-code-run",
        task_key="post_comments:101:202:g3",
        page_number=2,
        task_type="full_scan_comments",
        scope_type="post_comments",
        tid=101,
        parent_pid=202,
        generation=3,
        error=_CodeError(code),
        continuation_json={"page_non_top_cids": [303]},
    )

    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["status"] == status
    assert checkpoint["scope_status"] == scope_status
    assert checkpoint["error_code"] == stored_code
    assert checkpoint["error_type"] == "_CodeError"
    assert checkpoint["reason"] == reason
    assert checkpoint["next_page"] == 2


class _AsyncNoop:
    def __init__(self, state=None):
        self.state = state

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *_args):
        if self.state is not None and exc_type is None:
            self.state["committed"] = True
        return False


class _AsyncSessionNoop(_AsyncNoop):
    def begin(self):
        return _AsyncNoop(self.state)


def _comments_commit_handler(monkeypatch, enqueue_error: BaseException):
    from src.research import persistence

    async def persist_research_page(*_args, **_kwargs):
        return None

    monkeypatch.setattr(persistence, "persist_research_page", persist_research_page)
    parent_scope = SimpleNamespace(generation=2)
    comment_scope = SimpleNamespace(generation=3, parent_generation=2)
    transaction_state = {"committed": False}

    class AsyncSession(_AsyncSessionNoop):
        def __init__(self):
            super().__init__(transaction_state)

    async def enqueue_after_commit(*_args, **_kwargs):
        assert transaction_state["committed"] is True
        raise enqueue_error

    datastore = SimpleNamespace(
        async_sessionmaker=AsyncSession,
        get_research_task_scope_for_update=AsyncMock(side_effect=[parent_scope, comment_scope]),
        save_items_in_session=AsyncMock(),
        record_research_checkpoint=AsyncMock(return_value=True),
        remove_pending_comment_scan_in_session=AsyncMock(),
    )
    handler = _comments_handler(datastore)
    handler.ensure_users = AsyncMock()
    handler._prepare_research_page = AsyncMock()
    handler._enqueue_missing_web_targets = AsyncMock(side_effect=enqueue_after_commit)
    return handler, datastore, transaction_state


@pytest.mark.asyncio
async def test_postcommit_web_enqueue_error_does_not_rewrite_completed_api_checkpoint(monkeypatch):
    handler, datastore, transaction_state = _comments_commit_handler(
        monkeypatch, RuntimeError("synthetic web enqueue error")
    )
    page = make_comments_response(
        [make_comment(cid=303, pid=202, tid=101, fid=9, fname="synthetic")]
    )
    task = FullScanCommentsTask(
        tid=101,
        pid=202,
        run_id="synthetic-postcommit-run",
        generation=3,
        parent_generation=2,
        fid=9,
        fname="synthetic",
        parent_kind="reply_post",
    )

    committed = await handler._commit_research_comments_page(
        task,
        page,
        object(),
        page_number=1,
        cursor={},
        attempts=1,
    )

    assert committed is True
    assert transaction_state["committed"] is True
    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["status"] == "completed"
    assert checkpoint["scope_status"] == "completed"
    handler._enqueue_missing_web_targets.assert_awaited_once()


@pytest.mark.asyncio
async def test_postcommit_web_enqueue_cancellation_still_propagates(monkeypatch):
    handler, datastore, transaction_state = _comments_commit_handler(monkeypatch, asyncio.CancelledError())
    page = make_comments_response(
        [make_comment(cid=303, pid=202, tid=101, fid=9, fname="synthetic")]
    )
    task = FullScanCommentsTask(
        tid=101,
        pid=202,
        run_id="synthetic-postcommit-cancel-run",
        generation=3,
        parent_generation=2,
        fid=9,
        fname="synthetic",
        parent_kind="reply_post",
    )

    with pytest.raises(asyncio.CancelledError):
        await handler._commit_research_comments_page(
            task,
            page,
            object(),
            page_number=1,
            cursor={},
            attempts=1,
        )

    checkpoint = datastore.record_research_checkpoint.await_args.kwargs
    assert checkpoint["status"] == "completed"
    assert checkpoint["scope_status"] == "completed"
    assert transaction_state["committed"] is True
