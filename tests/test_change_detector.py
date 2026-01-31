"""Tests for the change detection system."""

from src.pipeline.change_detector import detect_changes, build_change_summary


class TestDetectChanges:
    def test_all_new(self):
        changes = detect_changes(set(), {"a", "b", "c"})
        assert changes["added"] == {"a", "b", "c"}
        assert changes["removed"] == set()
        assert changes["retained"] == set()

    def test_all_removed(self):
        changes = detect_changes({"a", "b"}, set())
        assert changes["added"] == set()
        assert changes["removed"] == {"a", "b"}

    def test_mixed(self):
        changes = detect_changes({"a", "b", "c"}, {"b", "c", "d"})
        assert changes["added"] == {"d"}
        assert changes["removed"] == {"a"}
        assert changes["retained"] == {"b", "c"}

    def test_no_change(self):
        changes = detect_changes({"a", "b"}, {"a", "b"})
        assert changes["added"] == set()
        assert changes["removed"] == set()
        assert changes["retained"] == {"a", "b"}


class TestBuildSummary:
    def test_summary_counts(self):
        changes = {"added": {"d"}, "removed": {"a"}, "retained": {"b", "c"}}
        summary = build_change_summary(changes)
        assert summary["added_count"] == 1
        assert summary["removed_count"] == 1
        assert summary["retained_count"] == 2
        assert summary["total_count"] == 3
