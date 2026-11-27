"""Review counts must distinguish an empty composition from a rejected one."""

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location(
    "composition_review", ROOT / "scripts/review_ibm_analytics.py"
)
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


def database():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    review.initialize(db)
    return db


def source(identifier="composition-1", template="template-1"):
    return {
        "_id": identifier,
        "template": template,
        "creation_date": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "cn": [{}, {}],
    }


def projected(count):
    return SimpleNamespace(
        status="projected",
        facts=[{"_id": i, "clinical": {"bp": {"systolic": i}}} for i in range(count)],
        activity={"_id": "activity"},
        source_ref={"_id": "ref"},
        state=lambda: {"status": "projected"},
    )


def test_rejection_has_no_fact_count_and_no_partial_projection():
    with database() as db:
        review.save_result(
            db, source(), None, ValueError("A related subject needs mapping"), (2, 0)
        )
        review.save_result(db, source("composition-2"), projected(0), None, (0, 0))
        rows = review.read_rows(db, {})["rows"]
        assert rows[0]["fact_count"] is None
        assert rows[0]["status"] == "rejected"
        assert rows[1]["fact_count"] == 0
        totals = review.summarize(db)["totals"]
        assert (
            totals["converted"],
            totals["zero_facts"],
            totals["rejected"],
            totals["facts"],
        ) == (1, 1, 1, 0)
        assert review.read_detail(db, 1)["projection"] == {}
        assert review.read_detail(db, 1)["facts"] == []


def test_detail_pages_actual_facts_without_truncating_the_count():
    with database() as db:
        review.save_result(db, source(), projected(41), None, (82, 0))
        assert review.read_detail(db, 1)["fact_count"] == 41
        assert len(review.read_detail(db, 1)["facts"]) == 20
        assert review.read_detail(db, 1, 2)["facts"][0]["_id"] == 40
        assert review.read_detail(db, 2) is None


def test_filters_pagination_and_quoted_identifiers():
    with database() as db:
        for i in range(53):
            review.save_result(
                db, source(str(i), "template-1"), projected(i % 2), None, (i % 2, 0)
            )
        review.save_result(
            db, source("a' OR 1=1 --", "template-2"), projected(2), None, (2, 0)
        )
        assert review.read_rows(db, {})["total"] == 54
        assert len(review.read_rows(db, {"page": "1"})["rows"]) == 4
        assert review.read_rows(db, {"zero": "1"})["total"] == 27
        rows = review.read_rows(db, {"q": "a' OR 1=1 --"})["rows"]
        assert len(rows) == 1 and rows[0]["template"] == "template-2"
        assert (
            review.read_rows(db, {"template": "template-2", "sort": "facts"})["total"]
            == 1
        )
        assert review.read_rows(db, {"status": "rejected"})["total"] == 0


class Cursor:
    def __init__(self, documents, checks):
        self.documents, self.checks = list(documents), checks

    def hint(self, name):
        assert name == review.INDEX
        self.checks.append("index")
        return self

    def sort(self, name, direction):
        self.documents.sort(key=lambda d: d[name], reverse=direction < 0)
        return self

    def limit(self, count):
        assert count > 0
        self.documents = self.documents[:count]
        self.checks.append(count)
        return self

    def batch_size(self, count):
        return self

    def max_time_ms(self, count):
        assert count == 15000
        self.checks.append("time_budget")
        return self

    def __enter__(self):
        self.iterator = iter(self.documents)
        return self

    def __exit__(self, *args):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.iterator)


class Source:
    def __init__(self, documents):
        self.documents, self.checks = documents, []

    def find(self, criteria, projection):
        docs = [d for d in self.documents if d["template"] == criteria["template"]]
        if "creation_date" in criteria:
            docs = [
                d
                for d in docs
                if d["creation_date"] >= criteria["creation_date"]["$gte"]
            ]
        return Cursor(docs, self.checks)


def test_overlapping_date_windows_deduplicate_and_fill_to_cap():
    collection = Source([source(str(i)) for i in range(40)])
    docs = list(review.sampled_documents(collection, "template-1", 25))
    assert len(docs) == len({d["_id"] for d in docs}) == 25
    assert collection.checks.count("index") == collection.checks.count("time_budget")
    assert max(x for x in collection.checks if isinstance(x, int)) <= 50


def test_small_template_exhaustion_is_not_padded_with_duplicates():
    collection = Source([source(str(i)) for i in range(4)])
    assert len(list(review.sampled_documents(collection, "template-1", 25))) == 4
    assert list(review.sampled_documents(collection, "absent", 25)) == []
