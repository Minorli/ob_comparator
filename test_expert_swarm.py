import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import expert_swarm as es


class TestExpertSwarm(unittest.TestCase):
    def test_blueprint_contains_required_experts(self):
        blueprint = es.build_swarm_blueprint(project_root=Path("/tmp/comparator"))
        role_ids = {role["role_id"] for role in blueprint["roles"]}

        self.assertIn("chief_architect", role_ids)
        self.assertIn("principal_code_reviewer", role_ids)
        self.assertIn("principal_programming_expert", role_ids)
        self.assertIn("principal_database_expert", role_ids)

    def test_blueprint_contains_repository_guardrails(self):
        blueprint = es.build_swarm_blueprint(project_root=Path("/tmp/comparator"))
        guardrails = blueprint["guardrails"]

        self.assertTrue(any("config.ini.template" in item and "readme_config.txt" in item for item in guardrails))
        self.assertTrue(any("output-only" in item and "fixup" in item for item in guardrails))
        self.assertTrue(any("deterministic" in item for item in guardrails))

    def test_blueprint_contains_parallel_subteams_and_arbiter(self):
        blueprint = es.build_swarm_blueprint(project_root=Path("/tmp/comparator"))

        self.assertEqual(blueprint["execution_pattern"], "parallel_subteams_with_arbiter")
        self.assertGreaterEqual(len(blueprint["subteams"]), 2)
        self.assertEqual(blueprint["arbiter"]["role_id"], "swarm_arbiter")
        self.assertEqual(blueprint["arbiter"]["output_path"], "audit/swarm/consolidated_report.md")

    def test_subteams_cover_roles_exactly_once(self):
        blueprint = es.build_swarm_blueprint(project_root=Path("/tmp/comparator"))
        es._validate_subteam_memberships(blueprint)

        role_ids = {role["role_id"] for role in blueprint["roles"]}
        assigned = []
        for team in blueprint["subteams"]:
            assigned.extend(team["member_role_ids"])

        self.assertEqual(set(assigned), role_ids)
        self.assertEqual(len(assigned), len(role_ids))

    def test_subteam_output_paths_are_under_swarm_dir(self):
        blueprint = es.build_swarm_blueprint(project_root=Path("/tmp/comparator"))
        for team in blueprint["subteams"]:
            self.assertTrue(team["output_path"].startswith("audit/swarm/"))
            self.assertIn("subteam_", team["output_path"])

    def test_writers_create_files(self):
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            blueprint = es.build_swarm_blueprint(project_root=temp_root)
            blueprint_path = temp_root / "audit/swarm/swarm_blueprint.json"
            task_path = temp_root / "audit/swarm/swarm_task.md"

            es.write_swarm_blueprint(blueprint_path, blueprint)
            es.write_task_brief(task_path, "hello swarm")

            self.assertTrue(blueprint_path.exists())
            self.assertTrue(task_path.exists())

            payload = json.loads(blueprint_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["project"], "comparator")
            self.assertEqual(task_path.read_text(encoding="utf-8").strip(), "hello swarm")

    def test_write_swarm_blueprint_preserves_unicode(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "swarm.json"
            es.write_swarm_blueprint(path, {"summary": "中文结论"})
            text = path.read_text(encoding="utf-8")
            self.assertIn("中文结论", text)
            self.assertNotIn("\\u4e2d", text)

    def test_load_task_text_rejects_conflicting_inputs(self):
        with self.assertRaises(ValueError):
            es._load_task_text(task="a", task_file="b.md")

    def test_run_role_agent_aggregates_all_messages(self):
        async def fake_query_fn(**_kwargs):
            for item in ("first analysis", "final summary"):
                yield item

        blueprint = es.build_swarm_blueprint(project_root=Path("/tmp/comparator"))
        role = blueprint["roles"][0]
        result = asyncio.run(
            es._run_role_agent(
                role=role,
                subteam_name="demo",
                task_text="task",
                blueprint=blueprint,
                model="m",
                max_turns=1,
                query_fn=fake_query_fn,
            )
        )

        self.assertEqual(result["summary"], "first analysis\n\nfinal summary")

    def test_run_subteam_preserves_successful_member_when_one_fails(self):
        blueprint = es.build_swarm_blueprint(project_root=Path("/tmp/comparator"))
        role_map = {role["role_id"]: role for role in blueprint["roles"]}
        subteam = blueprint["subteams"][0]

        async def fake_run_role_agent(**kwargs):
            role = kwargs["role"]
            if role["role_id"] == subteam["member_role_ids"][0]:
                return {
                    "role_id": role["role_id"],
                    "name": role["name"],
                    "output_path": role["output_path"],
                    "summary": "ok summary",
                }
            raise RuntimeError("boom")

        async def fake_query_fn(**_kwargs):
            yield "subteam summary"

        with mock.patch.object(es, "_run_role_agent", side_effect=fake_run_role_agent):
            result = asyncio.run(
                es._run_subteam(
                    subteam=subteam,
                    role_map=role_map,
                    task_text="task",
                    blueprint=blueprint,
                    model="m",
                    max_turns=1,
                    query_fn=fake_query_fn,
                )
            )

        self.assertEqual(result["summary"], "subteam summary")
        self.assertEqual(len(result["member_summaries"]), len(subteam["member_role_ids"]))
        self.assertTrue(any(item.get("summary") == "ok summary" for item in result["member_summaries"]))
        self.assertTrue(any(item.get("error") == "boom" for item in result["member_summaries"]))

    def test_execute_swarm_preserves_successful_subteams_when_one_fails(self):
        blueprint = es.build_swarm_blueprint(project_root=Path("/tmp/comparator"))

        async def fake_run_subteam(**kwargs):
            subteam = kwargs["subteam"]
            if subteam["team_id"] == blueprint["subteams"][0]["team_id"]:
                return {"team_id": subteam["team_id"], "name": subteam["name"], "summary": "ok"}
            raise RuntimeError("subteam boom")

        async def fake_run_arbiter(**kwargs):
            summaries = kwargs["subteam_summaries"]
            self.assertTrue(any(item.get("summary") == "ok" for item in summaries))
            self.assertTrue(any(item.get("error") == "subteam boom" for item in summaries))
            return "arbiter summary"

        with mock.patch.object(es, "_load_claude_sdk", return_value=object()), \
             mock.patch.object(es, "_run_subteam", side_effect=fake_run_subteam), \
             mock.patch.object(es, "_run_arbiter", side_effect=fake_run_arbiter), \
             mock.patch.dict("os.environ", {"ANTHROPIC_API_KEY": "x"}, clear=False):
            result = asyncio.run(es.execute_swarm(blueprint, "task", "m", 1))

        self.assertEqual(result, "arbiter summary")


if __name__ == "__main__":
    unittest.main()
